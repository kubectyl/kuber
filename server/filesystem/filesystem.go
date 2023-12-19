package filesystem

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"emperror.dev/errors"
	"github.com/gabriel-vasile/mimetype"
	"github.com/karrick/godirwalk"
	"github.com/pkg/sftp"
	ignore "github.com/sabhiram/go-gitignore"
	"golang.org/x/crypto/ssh"

	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/environment"
	"github.com/kubectyl/kuber/system"
)

type Filesystem struct {
	mu                sync.RWMutex
	lastLookupTime    *usageLookupTime
	lookupInProgress  *system.AtomicBool
	diskUsed          int64
	diskCheckInterval time.Duration
	denylist          *ignore.GitIgnore

	// The maximum amount of disk space (in bytes) that this Filesystem instance can use.
	diskLimit int64

	// The root data directory path for this Filesystem instance.
	root string

	isTest bool

	// SFTP Manager
	manager *BasicSFTPManager
}

var (
	clientConfig *ssh.ClientConfig
)

// New creates a new Filesystem instance for a given server.
func New(root string, size int64, denylist []string, addr string) *Filesystem {
	key, err := os.ReadFile(environment.PrivateKeyPath())
	if err != nil {
		log.Fatalf("Unable to read private key: %v", err)
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		log.Fatalf("Unable to parse private key: %v", err)
	}
	clientConfig = &ssh.ClientConfig{
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			// use OpenSSH's known_hosts file if you care about host validation
			return nil
		},
		Timeout: time.Second * 5,
	}

	return &Filesystem{
		root:              "/",
		diskLimit:         size,
		diskCheckInterval: time.Duration(config.Get().System.DiskCheckInterval),
		lastLookupTime:    &usageLookupTime{},
		lookupInProgress:  system.NewAtomicBool(false),
		denylist:          ignore.CompileIgnoreLines(denylist...),
		manager:           NewBasicSFTPManager(addr, clientConfig),
	}
}

func (fs *Filesystem) SetManager(addr string) {
	fs.mu.Lock()
	fs.manager = NewBasicSFTPManager(addr, clientConfig)
	fs.mu.Unlock()
}

// Path returns the root path for the Filesystem instance.
func (fs *Filesystem) Path() string {
	return fs.root
}

// File returns a reader for a file instance as well as the stat information.
func (fs *Filesystem) File(p string) (*sftp.File, os.FileInfo, error) {
	cleaned, err := fs.SafePath(p)
	if err != nil {
		return nil, Stat{}, errors.WithStackIf(err)
	}
	st, err := fs.manager.Stat(cleaned)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, Stat{}, newFilesystemError(ErrNotExist, err)
		}
		return nil, Stat{}, errors.WithStackIf(err)
	}
	if st.IsDir() {
		return nil, Stat{}, newFilesystemError(ErrCodeIsDirectory, nil)
	}
	f, err := fs.manager.Open(cleaned)
	if err != nil {
		return nil, Stat{}, errors.WithStackIf(err)
	}
	return f, st, nil
}

// Touch acts by creating the given file and path on the disk if it is not present
// already. If  it is present, the file is opened using the defaults which will truncate
// the contents. The opened file is then returned to the caller.
func (fs *Filesystem) Touch(p string, flag int) (*sftp.File, error) {
	cleaned, err := fs.SafePath(p)
	if err != nil {
		return nil, err
	}

	f, err := fs.manager.OpenFile(cleaned, flag)
	if err == nil {
		return f, nil
	}
	// If the error is not because it doesn't exist then we just need to bail at this point.
	if !errors.Is(err, os.ErrNotExist) {
		return nil, errors.Wrap(err, "server/filesystem: touch: failed to open file handle")
	}
	// Only create and chown the directory if it doesn't exist.
	if _, err := fs.manager.Stat(filepath.Dir(cleaned)); errors.Is(err, os.ErrNotExist) {
		// Create the path leading up to the file we're trying to create, setting the final perms
		// on it as we go.
		if err := fs.manager.MkdirAll(filepath.Dir(cleaned)); err != nil {
			return nil, errors.Wrap(err, "server/filesystem: touch: failed to create directory tree")
		}
		if err := fs.Chown(filepath.Dir(cleaned)); err != nil {
			return nil, err
		}
	}
	// o := &fileOpener{}
	// Try to open the file now that we have created the pathing necessary for it, and then
	// Chown that file so that the permissions don't mess with things.
	// f, err = o.open(cleaned, flag, 0o644)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "server/filesystem: touch: failed to open file with wait")
	// }
	_ = fs.Chown(cleaned)
	return f, nil
}

// Writefile writes a file to the system. If the file does not already exist one
// will be created. This will also properly recalculate the disk space used by
// the server when writing new files or modifying existing ones.
func (fs *Filesystem) Writefile(p string, r io.Reader) error {
	cleaned, err := fs.SafePath(p)
	if err != nil {
		return err
	}

	var currentSize int64
	// If the file does not exist on the system already go ahead and create the pathway
	// to it and an empty file. We'll then write to it later on after this completes.
	stat, err := fs.manager.Stat(cleaned)
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "server/filesystem: writefile: failed to stat file")
	} else if err == nil {
		if stat.IsDir() {
			return errors.WithStack(&Error{code: ErrCodeIsDirectory, resolved: cleaned})
		}
		currentSize = stat.Size()
	}

	// br := bufio.NewReader(r)
	// Check that the new size we're writing to the disk can fit. If there is currently
	// a file we'll subtract that current file size from the size of the buffer to determine
	// the amount of new data we're writing (or amount we're removing if smaller).
	// if err := fs.HasSpaceFor(int64(br.Size()) - currentSize); err != nil {
	// 	return err
	// }

	// Touch the file and return the handle to it at this point. This will create the file,
	// any necessary directories, and set the proper owner of the file.
	file, err := fs.Touch(cleaned, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, 1024*4)
	sz, err := io.CopyBuffer(file, r, buf)

	// Adjust the disk usage to account for the old size and the new size of the file.
	fs.addDisk(sz - currentSize)

	return fs.Chown(cleaned)
	// return nil
}

// Creates a new directory (name) at a specified path (p) for the server.
func (fs *Filesystem) CreateDirectory(name string, p string) error {
	cleaned, err := fs.SafePath(path.Join(p, name))
	if err != nil {
		return err
	}
	return fs.manager.MkdirAll(cleaned)
}

// Rename moves (or renames) a file or directory.
func (fs *Filesystem) Rename(from string, to string) error {
	cleanedFrom, err := fs.SafePath(from)
	if err != nil {
		return errors.WithStack(err)
	}

	cleanedTo, err := fs.SafePath(to)
	if err != nil {
		return errors.WithStack(err)
	}

	// If the target file or directory already exists the rename function will fail, so just
	// bail out now.
	if _, err := fs.manager.Stat(cleanedTo); err == nil {
		return os.ErrExist
	}

	if cleanedTo == fs.Path() {
		return errors.New("attempting to rename into an invalid directory space")
	}

	d := strings.TrimSuffix(cleanedTo, path.Base(cleanedTo))
	// Ensure that the directory we're moving into exists correctly on the system. Only do this if
	// we're not at the root directory level.
	if d != fs.Path() {
		if mkerr := fs.manager.MkdirAll(d); mkerr != nil {
			return errors.WithMessage(mkerr, "failed to create directory structure for file rename")
		}
	}

	if err := fs.manager.Rename(cleanedFrom, cleanedTo); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Recursively iterates over a file or directory and sets the permissions on all of the
// underlying files. Iterate over all of the files and directories. If it is a file just
// go ahead and perform the chown operation. Otherwise dig deeper into the directory until
// we've run out of directories to dig into.
func (fs *Filesystem) Chown(path string) error {
	cleaned, err := fs.SafePath(path)
	if err != nil {
		return err
	}

	if fs.isTest {
		return nil
	}

	// Start by just chowning the initial path that we received.
	if err := fs.manager.Chown(cleaned); err != nil {
		return errors.Wrap(err, "server/filesystem: chown: failed to chown path")
	}

	// If this is not a directory we can now return from the function, there is nothing
	// left that we need to do.
	if st, err := fs.manager.Stat(cleaned); err != nil || !st.IsDir() {
		return nil
	}

	// If this was a directory, begin walking over its contents recursively and ensure that all
	// of the subfiles and directories get their permissions updated as well.
	err = godirwalk.Walk(cleaned, &godirwalk.Options{
		Unsorted: true,
		Callback: func(p string, e *godirwalk.Dirent) error {
			// Do not attempt to chown a symlink. Go's os.Chown function will affect the symlink
			// so if it points to a location outside the data directory the user would be able to
			// (un)intentionally modify that files permissions.
			if e.IsSymlink() {
				if e.IsDir() {
					return godirwalk.SkipThis
				}

				return nil
			}

			return fs.manager.Chown(p)
		},
	})

	// return errors.Wrap(err, "server/filesystem: chown: failed to chown during walk function")
	return nil
}

func (fs *Filesystem) Chmod(path string, mode os.FileMode) error {
	cleaned, err := fs.SafePath(path)
	if err != nil {
		return err
	}

	if fs.isTest {
		return nil
	}

	if err := fs.manager.Chmod(cleaned, mode); err != nil {
		return err
	}

	return nil
}

// Begin looping up to 50 times to try and create a unique copy file name. This will take
// an input of "file.txt" and generate "file copy.txt". If that name is already taken, it will
// then try to write "file copy 2.txt" and so on, until reaching 50 loops. At that point we
// won't waste anymore time, just use the current timestamp and make that copy.
//
// Could probably make this more efficient by checking if there are any files matching the copy
// pattern, and trying to find the highest number and then incrementing it by one rather than
// looping endlessly.
func (fs *Filesystem) findCopySuffix(dir string, name string, extension string) (string, error) {
	var i int
	suffix := " copy"

	for i = 0; i < 51; i++ {
		if i > 0 {
			suffix = " copy " + strconv.Itoa(i)
		}

		n := name + suffix + extension
		// If we stat the file and it does not exist that means we're good to create the copy. If it
		// does exist, we'll just continue to the next loop and try again.
		if _, err := fs.manager.Stat(path.Join(dir, n)); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return "", err
			}

			break
		}

		if i == 50 {
			suffix = "copy." + time.Now().Format(time.RFC3339)
		}
	}

	return name + suffix + extension, nil
}

// Copies a given file to the same location and appends a suffix to the file to indicate that
// it has been copied.
func (fs *Filesystem) Copy(p string) error {
	cleaned, err := fs.SafePath(p)
	if err != nil {
		return err
	}

	s, err := fs.manager.Stat(cleaned)
	if err != nil {
		return err
	} else if s.IsDir() || !s.Mode().IsRegular() {
		// If this is a directory or not a regular file, just throw a not-exist error
		// since anything calling this function should understand what that means.
		return os.ErrNotExist
	}

	// Check that copying this file wouldn't put the server over its limit.
	if err := fs.HasSpaceFor(s.Size()); err != nil {
		return err
	}

	base := filepath.Base(cleaned)
	relative := strings.TrimSuffix(strings.TrimPrefix(cleaned, fs.Path()), base)
	extension := filepath.Ext(base)
	name := strings.TrimSuffix(base, extension)

	// Ensure that ".tar" is also counted as apart of the file extension.
	// There might be a better way to handle this for other double file extensions,
	// but this is a good workaround for now.
	if strings.HasSuffix(name, ".tar") {
		extension = ".tar" + extension
		name = strings.TrimSuffix(name, ".tar")
	}

	source, err := fs.manager.Open(cleaned)
	if err != nil {
		return err
	}
	defer source.Close()

	n, err := fs.findCopySuffix(relative, name, extension)
	if err != nil {
		return err
	}

	return fs.Writefile(path.Join(relative, n), source)
}

// TruncateRootDirectory removes _all_ files and directories from a server's
// data directory and resets the used disk space to zero.
func (fs *Filesystem) TruncateRootDirectory() error {
	if err := fs.manager.Remove(fs.Path()); err != nil {
		return err
	}
	if err := fs.manager.Mkdir(fs.Path()); err != nil {
		return err
	}
	atomic.StoreInt64(&fs.diskUsed, 0)
	return nil
}

// Delete removes a file or folder from the system. Prevents the user from
// accidentally (or maliciously) removing their root server data directory.
func (fs *Filesystem) Delete(p string) error {
	wg := sync.WaitGroup{}
	// This is one of the few (only?) places in the codebase where we're explicitly not using
	// the SafePath functionality when working with user provided input. If we did, you would
	// not be able to delete a file that is a symlink pointing to a location outside of the data
	// directory.
	//
	// We also want to avoid resolving a symlink that points _within_ the data directory and thus
	// deleting the actual source file for the symlink rather than the symlink itself. For these
	// purposes just resolve the actual file path using filepath.Join() and confirm that the path
	// exists within the data directory.
	resolved := fs.unsafeFilePath(p)
	// if !fs.unsafeIsInDataDirectory(resolved) {
	// 	return NewBadPathResolution(p, resolved)
	// }

	// Block any whoopsies.
	if resolved == fs.Path() {
		return errors.New("cannot delete root server directory")
	}

	if st, err := fs.manager.Lstat(resolved); err != nil {
		if !os.IsNotExist(err) {
			fs.error(err).Warn("error while attempting to stat file before deletion")
		}
	} else {
		if !st.IsDir() {
			fs.addDisk(-st.Size())
		} else {
			wg.Add(1)
			go func(wg *sync.WaitGroup, st os.FileInfo, resolved string) {
				defer wg.Done()
				if s, err := fs.directorySize(resolved); err == nil {
					fs.addDisk(-s)
				}
			}(&wg, st, resolved)

			return fs.deleteRecursive(resolved)
		}
	}

	wg.Wait()

	return fs.manager.Remove(resolved)
}

func (fs *Filesystem) deleteRecursive(name string) error {
	entries, err := fs.manager.ReadDir(name)
	if err != nil {
		return errors.Wrap(err, "ReadDir")
	}

	if len(entries) == 0 {
		err = fs.manager.RemoveDirectory(name)
		if err != nil {
			return errors.Wrap(err, "RemoveDirectory")
		}
	}

	for _, fi := range entries {
		itemName, _ := fs.manager.Join(name, fi.Name())
		if fi.IsDir() {
			err = fs.deleteRecursive(itemName)
			if err != nil {
				return errors.Wrap(err, "ReadDir")
			}

			err = fs.manager.RemoveDirectory(name)
			if err != nil {
				return errors.Wrap(err, "RemoveDirectory")
			}

			continue
		}

		err := fs.manager.Remove(itemName)
		if err != nil {
			return errors.Wrap(err, "ReadDir")
		}
	}

	return nil
}

// ListDirectory lists the contents of a given directory and returns stat
// information about each file and folder within it.
func (fs *Filesystem) ListDirectory(p string) ([]Stat, error) {
	cleaned, err := fs.SafePath(p)
	if err != nil {
		return nil, err
	}

	files, err := fs.manager.ReadDir(cleaned)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup

	// You must initialize the output of this directory as a non-nil value otherwise
	// when it is marshaled into a JSON object you'll just get 'null' back, which will
	// break the panel badly.
	out := make([]Stat, len(files))

	// Iterate over all of the files and directories returned and perform an async process
	// to get the mime-type for them all.
	for i, file := range files {
		wg.Add(1)

		go func(idx int, f os.FileInfo) {
			defer wg.Done()

			var m *mimetype.MIME
			d := "inode/directory"
			if !f.IsDir() {
				cleanedp := filepath.Join(cleaned, f.Name())
				if f.Mode()&os.ModeSymlink != 0 {
					cleanedp = filepath.Join(cleaned, f.Name())
				}

				// Don't try to detect the type on a pipe — this will just hang the application and
				// you'll never get a response back.
				//
				// @see https://github.com/pterodactyl/panel/issues/4059
				if cleanedp != "" && f.Mode()&os.ModeNamedPipe == 0 {
					file, err := fs.manager.Open(filepath.Join(cleaned, f.Name()))
					if err != nil {
						// panic(fmt.Errorf("Error SFTP Open: %s", err))
						fmt.Println(err)
					}
					m, _ = mimetype.DetectReader(file)
				} else {
					// Just pass this for an unknown type because the file could not safely be resolved within
					// the server data path.
					d = "application/octet-stream"
				}
			}

			st := Stat{FileInfo: f, Mimetype: d}
			if m != nil {
				st.Mimetype = m.String()
			}
			out[idx] = st
		}(i, file)
	}

	wg.Wait()

	// Sort the output alphabetically to begin with since we've run the output
	// through an asynchronous process and the order is gonna be very random.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Name() == out[j].Name() || out[i].Name() > out[j].Name() {
			return true
		}
		return false
	})

	// Then, sort it so that directories are listed first in the output. Everything
	// will continue to be alphabetized at this point.
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].IsDir()
	})

	return out, nil
}

func (fs *Filesystem) Chtimes(path string, atime, mtime time.Time) error {
	cleaned, err := fs.SafePath(path)
	if err != nil {
		return err
	}

	if fs.isTest {
		return nil
	}

	if err := fs.manager.Chtimes(cleaned, atime, mtime); err != nil {
		return err
	}

	return nil
}
