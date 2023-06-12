package filesystem

import (
	"archive/tar"
	"archive/zip"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"emperror.dev/errors"
	"github.com/gabriel-vasile/mimetype"
	"github.com/nwaples/rardecode/v2"
	"github.com/pkg/sftp"
	"github.com/ulikunitz/xz"
)

func (fs *Filesystem) createArchive(cleaned []string, archivePath string) error {
	archiveFile, err := fs.manager.Create(archivePath)
	if err != nil {
		return fmt.Errorf("failed to create archive file: %v", err)
	}
	defer archiveFile.Close()

	// Create a gzip writer
	gzipWriter := gzip.NewWriter(archiveFile)
	defer gzipWriter.Close()

	// Create a tar writer using the gzip writer
	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	for _, remoteFile := range cleaned {
		fileInfo, err := fs.manager.Stat(remoteFile)
		if err != nil {
			log.Printf("Failed to retrieve file info for %q: %v", remoteFile, err)
			continue
		}

		if fileInfo.IsDir() {
			err = fs.addDirectoryToArchive(remoteFile, tarWriter)
			if err != nil {
				log.Printf("Failed to add directory %q to archive: %v", remoteFile, err)
			}
			continue
		}

		remoteReader, err := fs.manager.Open(remoteFile)
		if err != nil {
			log.Printf("Failed to open remote file %q: %v", remoteFile, err)
			continue
		}
		defer remoteReader.Close()

		header := &tar.Header{
			Name:    remoteFile,
			Size:    fileInfo.Size(),
			Mode:    int64(fileInfo.Mode().Perm()),
			ModTime: fileInfo.ModTime(),
		}

		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to add file %q to archive: %v", remoteFile, err)
		}

		if _, err := io.Copy(tarWriter, remoteReader); err != nil {
			return fmt.Errorf("failed to compress file %q: %v", remoteFile, err)
		}
	}

	return nil
}

func (fs *Filesystem) addDirectoryToArchive(remoteDir string, archiveWriter *tar.Writer) error {
	entries, err := fs.manager.ReadDir(remoteDir)
	if err != nil {
		return fmt.Errorf("failed to read directory %q: %v", remoteDir, err)
	}

	for _, entry := range entries {
		remotePath := filepath.Join(remoteDir, entry.Name())

		if entry.IsDir() {
			err = fs.addDirectoryToArchive(remotePath, archiveWriter)
			if err != nil {
				return err
			}
			continue
		}

		remoteReader, err := fs.manager.Open(remotePath)
		if err != nil {
			log.Printf("Failed to open remote file %q: %v", remotePath, err)
			continue
		}
		defer remoteReader.Close()

		header := &tar.Header{
			Name:    remotePath,
			Size:    entry.Size(),
			Mode:    int64(entry.Mode().Perm()),
			ModTime: entry.ModTime(),
		}

		if err := archiveWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to add file %q to archive: %v", remotePath, err)
		}

		if _, err := io.Copy(archiveWriter, remoteReader); err != nil {
			return fmt.Errorf("failed to compress file %q: %v", remotePath, err)
		}
	}

	return nil
}

// CompressFiles compresses all the files matching the given paths in the
// specified directory. This function also supports passing nested paths to only
// compress certain files and folders when working in a larger directory. This
// effectively creates a local backup, but rather than ignoring specific files
// and folders, it takes an allow-list of files and folders.
//
// All paths are relative to the dir that is passed in as the first argument,
// and the compressed file will be placed at that location named
// `archive-{date}.tar.gz`.
func (fs *Filesystem) CompressFiles(dir string, paths []string) (os.FileInfo, error) {
	cleanedRootDir, err := fs.SafePath(dir)
	if err != nil {
		return nil, err
	}

	// Take all the paths passed in and merge them together with the root directory we've gotten.
	for i, p := range paths {
		paths[i] = filepath.Join(cleanedRootDir, p)
	}

	cleaned, err := fs.ParallelSafePath(paths)
	if err != nil {
		return nil, err
	}

	d := path.Join(
		cleanedRootDir,
		fmt.Sprintf("archive-%s.tar.gz", strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "")),
	)

	if err := fs.createArchive(cleaned, d); err != nil {
		return nil, err
	}

	stat, err := fs.Stat(d)
	if err != nil {
		return nil, err
	}

	return stat, nil
}

// SpaceAvailableForDecompression looks through a given archive and determines
// if decompressing it would put the server over its allocated disk space limit.
func (fs *Filesystem) SpaceAvailableForDecompression(ctx context.Context, dir string, file string) error {
	// Don't waste time trying to determine this if we know the server will have the space for
	// it since there is no limit.
	if fs.MaxDisk() <= 0 {
		return nil
	}

	// source, err := fs.SafePath(filepath.Join(dir, file))
	// if err != nil {
	// 	return err
	// }

	// Get the cached size in a parallel process so that if it is not cached we are not
	// waiting an unnecessary amount of time on this call.
	dirSize, _ := fs.DiskUsage(false)

	var size int64
	err := fs.walkDirSFTP(".", func(path string, fileInfo os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			// Stop walking if the context is canceled.
			return ctx.Err()
		default:
			if fileInfo.IsDir() {
				return nil // Skip directories
			}
			if atomic.AddInt64(&size, fileInfo.Size())+dirSize > fs.MaxDisk() {
				return newFilesystemError(ErrCodeDiskSpace, nil)
			}
			return nil
		}
	})
	if err != nil {
		if sftpErr, ok := err.(*sftp.StatusError); ok {
			return fmt.Errorf("sftp error: %s", sftpErr.Error())
		}
		return err
	}

	return nil
}

type SFTPWalkerFunc func(path string, fileInfo os.FileInfo, err error) error

func (fs *Filesystem) walkDirSFTP(dirPath string, fn SFTPWalkerFunc) error {
	entries, err := fs.manager.ReadDir(dirPath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		subPath := filepath.Join(dirPath, entry.Name())

		if entry.IsDir() {
			err := fs.walkDirSFTP(subPath, fn)
			if err != nil {
				if err := fn(subPath, entry, err); err != nil && err != filepath.SkipDir {
					return err
				}
			}
		} else {
			err := fn(subPath, entry, nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// DecompressFile will decompress a file in a given directory by using the
// archiver tool to infer the file type and go from there. This will walk over
// all the files within the given archive and ensure that there is not a
// zip-slip attack being attempted by validating that the final path is within
// the server data directory.
func (fs *Filesystem) DecompressFile(ctx context.Context, dir string, file string) error {
	source, err := fs.SafePath(filepath.Join(dir, file))
	if err != nil {
		return err
	}
	return fs.DecompressFileUnsafe(ctx, dir, source)
}

// DecompressFileUnsafe will decompress any file on the local disk without checking
// if it is owned by the server.  The file will be SAFELY decompressed and extracted
// into the server's directory.
func (fs *Filesystem) DecompressFileUnsafe(ctx context.Context, dir string, file string) error {
	// Ensure that the archive actually exists on the system.
	if _, err := fs.Stat(file); err != nil {
		return errors.WithStack(err)
	}

	f, err := fs.manager.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	buffer := make([]byte, 512)

	n, err := f.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read remote file: %v", err)
	}
	if n == 0 {
		return nil
	}

	mime := mimetype.Detect(buffer)

	switch mime.String() {
	case "application/vnd.rar", "application/x-rar-compressed":
		return fs.extractRARArchive(file, dir)
	case "application/x-tar", "application/x-br", "application/x-lzip", "application/x-sz", "application/zstd":
		return fs.extractTARArchive(file, dir)
	case "application/x-xz":
		return fs.extractTARXZArchive(file, dir)
	case "application/x-bzip2":
		return fs.extractBZIP2Archive(file, dir)
	case "application/gzip", "application/x-gzip":
		return fs.extractGZArchive(file, dir)
	case "application/zip":
		return fs.extractZIPArchive(file, dir)
	default:
		return fmt.Errorf("unsupported archive format: %s", mime.String())
	}
}

// Extract RAR archive file from the remote SFTP server.
func (fs *Filesystem) extractRARArchive(remoteFilePath string, destinationDir string) error {
	remoteFile, err := fs.manager.Open(remoteFilePath)
	if err != nil {
		return fmt.Errorf("failed to open remote file: %v", err)
	}
	defer remoteFile.Close()

	archive, err := rardecode.NewReader(remoteFile)
	if err != nil {
		return fmt.Errorf("failed to create RAR reader: %v", err)
	}

	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read RAR header: %v", err)
		}

		if header.IsDir {
			destDir := filepath.Join(destinationDir, header.Name)
			err := fs.manager.MkdirAll(destDir)
			if err != nil {
				return fmt.Errorf("failed to create directory: %v", err)
			}
			continue
		}

		destFilePath := filepath.Join(destinationDir, header.Name)
		destFile, err := fs.manager.Create(destFilePath)
		if err != nil {
			return fmt.Errorf("failed to create file: %v", err)
		}
		defer destFile.Close()

		_, err = io.Copy(destFile, archive)
		if err != nil {
			return fmt.Errorf("failed to extract file: %v", err)
		}
	}

	return nil
}

// Extract TAR archive file from the remote SFTP server.
func (fs *Filesystem) extractTARArchive(remoteFilePath string, destinationDir string) error {
	remoteFile, err := fs.manager.Open(remoteFilePath)
	if err != nil {
		return fmt.Errorf("failed to open remote file: %v", err)
	}
	defer remoteFile.Close()

	tarReader := tar.NewReader(remoteFile)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read TAR header: %v", err)
		}

		destFilePath := filepath.Join(destinationDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			err := fs.manager.MkdirAll(destFilePath)
			if err != nil {
				return fmt.Errorf("failed to create directory: %v", err)
			}
		case tar.TypeReg:
			destFile, err := fs.manager.Create(destFilePath)
			if err != nil {
				return fmt.Errorf("failed to create file: %v", err)
			}

			_, err = io.Copy(destFile, tarReader)
			if err != nil {
				destFile.Close()
				return fmt.Errorf("failed to extract file: %v", err)
			}

			destFile.Close()
		}
	}

	return nil
}

// Extract GZ archive file from the remote SFTP server.
func (fs *Filesystem) extractGZArchive(remoteFilePath string, destinationDir string) error {
	remoteFile, err := fs.manager.Open(remoteFilePath)
	if err != nil {
		return fmt.Errorf("failed to open remote file: %v", err)
	}
	defer remoteFile.Close()

	gzipReader, err := gzip.NewReader(remoteFile)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read TAR header: %v", err)
		}

		destFilePath := filepath.Join(destinationDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			err := fs.manager.MkdirAll(destFilePath)
			if err != nil {
				return fmt.Errorf("failed to create directory: %v", err)
			}
		case tar.TypeReg:
			destFile, err := fs.manager.Create(destFilePath)
			if err != nil {
				return fmt.Errorf("failed to create file: %v", err)
			}

			_, err = io.Copy(destFile, tarReader)
			if err != nil {
				destFile.Close()
				return fmt.Errorf("failed to extract file: %v", err)
			}

			destFile.Close()
		}
	}

	return nil
}

// Extract TAR.XZ archive file from the remote SFTP server.
func (fs *Filesystem) extractTARXZArchive(remoteFilePath string, destinationDir string) error {
	remoteFile, err := fs.manager.Open(remoteFilePath)
	if err != nil {
		return fmt.Errorf("failed to open remote file: %v", err)
	}
	defer remoteFile.Close()

	xzReader, err := xz.NewReader(remoteFile)
	if err != nil {
		return fmt.Errorf("failed to create XZ reader: %v", err)
	}

	tarReader := tar.NewReader(xzReader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read TAR header: %v", err)
		}

		destFilePath := filepath.Join(destinationDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			err := fs.manager.MkdirAll(destFilePath)
			if err != nil {
				return fmt.Errorf("failed to create directory: %v", err)
			}
		case tar.TypeReg:
			destFile, err := fs.manager.Create(destFilePath)
			if err != nil {
				return fmt.Errorf("failed to create file: %v", err)
			}

			_, err = io.Copy(destFile, tarReader)
			if err != nil {
				destFile.Close()
				return fmt.Errorf("failed to extract file: %v", err)
			}

			destFile.Close()
		}
	}

	return nil
}

// Extract BZIP2 archive file from the remote SFTP server.
func (fs *Filesystem) extractBZIP2Archive(remoteFilePath string, destinationDir string) error {
	remoteFile, err := fs.manager.Open(remoteFilePath)
	if err != nil {
		return fmt.Errorf("failed to open remote file: %v", err)
	}
	defer remoteFile.Close()

	bzip2Reader := bzip2.NewReader(remoteFile)
	tarReader := tar.NewReader(bzip2Reader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read TAR header: %v", err)
		}

		destFilePath := filepath.Join(destinationDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			err := fs.manager.MkdirAll(destFilePath)
			if err != nil {
				return fmt.Errorf("failed to create directory: %v", err)
			}
		case tar.TypeReg:
			destFile, err := fs.manager.Create(destFilePath)
			if err != nil {
				return fmt.Errorf("failed to create file: %v", err)
			}

			_, err = io.Copy(destFile, tarReader)
			if err != nil {
				destFile.Close()
				return fmt.Errorf("failed to extract file: %v", err)
			}

			destFile.Close()
		}
	}

	return nil
}

// Extract ZIP archive file from the remote SFTP server.
func (fs *Filesystem) extractZIPArchive(remoteFilePath string, destinationDir string) error {
	remoteFile, err := fs.manager.Open(remoteFilePath)
	if err != nil {
		return fmt.Errorf("failed to open remote file: %v", err)
	}
	defer remoteFile.Close()

	remoteFileInfo, err := fs.manager.Stat(remoteFilePath)
	if err != nil {
		return err
	}

	zipReader, err := zip.NewReader(remoteFile, remoteFileInfo.Size())
	if err != nil {
		return fmt.Errorf("failed to create zip reader: %v", err)
	}

	for _, file := range zipReader.File {
		destFilePath := filepath.Join(destinationDir, file.Name)

		if file.FileInfo().IsDir() {
			err := fs.manager.MkdirAll(destFilePath)
			if err != nil {
				return fmt.Errorf("failed to create directory: %v", err)
			}
			continue
		}

		destFile, err := fs.manager.Create(destFilePath)
		if err != nil {
			return fmt.Errorf("failed to create file: %v", err)
		}
		defer destFile.Close()

		srcFile, err := fs.manager.Open(file.Name)
		if err != nil {
			return fmt.Errorf("failed to open file inside ZIP: %v", err)
		}
		defer srcFile.Close()

		_, err = io.Copy(destFile, srcFile)
		if err != nil {
			return fmt.Errorf("failed to extract file from ZIP: %v", err)
		}
	}

	return nil
}
