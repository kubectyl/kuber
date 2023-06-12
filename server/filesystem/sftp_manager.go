package filesystem

import (
	"fmt"
	"io/fs"

	"github.com/apex/log"

	"sync"
	"sync/atomic"
	"time"

	"emperror.dev/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// SFTPManager is an interface for managing SFTP connections
type SFTPManager interface {
	NewClient() (*SFTPConn, error)
	GetConnection() (*SFTPConn, error)
	SetLogger(logger *log.Logger)
	Close() error
}

// NewSFTPConn creates an SFTPConn, mostly used for testing and should not really be used otherwise.
func NewSFTPConn(client *ssh.Client, sftpClient *sftp.Client) *SFTPConn {
	return &SFTPConn{
		sshConn:    client,
		sftpClient: sftpClient,
		shutdown:   make(chan bool, 1),
		closed:     false,
		reconnects: 0,
	}
}

// SFTPConn is a wrapped *sftp.Client
type SFTPConn struct {
	sync.Mutex
	sshConn    *ssh.Client
	sftpClient *sftp.Client
	shutdown   chan bool
	closed     bool
	reconnects uint64
}

// GetClient returns the underlying *sftp.Client
func (s *SFTPConn) GetClient() *sftp.Client {
	s.Lock()
	defer s.Unlock()
	return s.sftpClient
}

// Close closes the underlying connections
func (s *SFTPConn) Close() error {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return fmt.Errorf("connection was already closed")
	}

	s.shutdown <- true
	s.closed = true
	s.sshConn.Close()
	return s.sshConn.Wait()
}

// BasicSFTPManager is implements SFTPManager and supports basic reconnection on disconnect
// for SFTPConn returned by NewClient
type BasicSFTPManager struct {
	conns      []*SFTPConn
	log        *log.Entry
	connString string
	sshConfig  *ssh.ClientConfig
}

// NewBasicSFTPManager returns a BasicSFTPManager
func NewBasicSFTPManager(connString string, config *ssh.ClientConfig) *BasicSFTPManager {
	manager := &BasicSFTPManager{
		conns:      make([]*SFTPConn, 0),
		connString: connString,
		sshConfig:  config,
		log:        log.WithFields(log.Fields{"sftp_manager": connString, "timeout": config.Timeout}),
	}
	return manager
}

func (m *BasicSFTPManager) handleReconnects(c *SFTPConn) {
	closed := make(chan error, 1)
	go func() {
		closed <- c.sshConn.Wait()
	}()

	select {
	case <-c.shutdown:
		c.sshConn.Close()
		break
	// case res := <-closed:
	case <-closed:
		// m.log.WithField("error", res).Error("connection closed, reconnecting...")
		conn, err := ssh.Dial("tcp", m.connString, m.sshConfig)
		if err != nil {
			// m.log.WithField("error", err).Error("failed to reconnect.")
			m.Close()
			return
		}

		sftpConn, err := sftp.NewClient(conn)
		if err != nil {
			m.Close()
			return
		}

		atomic.AddUint64(&c.reconnects, 1)
		c.Lock()
		c.sftpClient = sftpConn
		c.sshConn = conn
		c.Unlock()
		// Cool we have a new connection, keep going
		m.handleReconnects(c)
	}
}

// NewClient returns an SFTPConn and ensures the underlying connection reconnects on failure
func (m *BasicSFTPManager) NewClient() (*SFTPConn, error) {
	conn, err := ssh.Dial("tcp", m.connString, m.sshConfig)
	if err != nil {
		// return nil, fmt.Errorf("failed to dial ssh: %s", err)
		return nil, nil
	}

	sftpConn, err := sftp.NewClient(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sftp subsystem: %s", err)
	}

	wrapped := &SFTPConn{
		sshConn:    conn,
		sftpClient: sftpConn,
		shutdown:   make(chan bool, 1),
	}
	go m.handleReconnects(wrapped)
	m.conns = append(m.conns, wrapped)
	return wrapped, nil
}

// GetConnection returns one of the existing connections the manager knows about. If there
// is no connections, we create a new one instead.
func (m *BasicSFTPManager) GetConnection() (*SFTPConn, error) {
	if len(m.conns) > 0 {
		return m.conns[0], nil
	}
	return m.NewClient()
}

// Close closes all connections managed by this manager
func (m *BasicSFTPManager) Close() error {
	for _, c := range m.conns {
		c.Close()
	}
	m.conns = nil
	return nil
}

func (m *BasicSFTPManager) withConnectionCheck(fn func(*sftp.Client) (interface{}, error)) (interface{}, error) {
	connection, err := m.GetConnection()
	if err != nil {
		return nil, err
	}
	if connection == nil || connection.sftpClient == nil {
		return nil, errors.New("client connection is invalid")
	}
	return fn(connection.sftpClient)
}

func (m *BasicSFTPManager) Stat(p string) (fs.FileInfo, error) {
	result, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return client.Stat(p)
	})
	if err != nil {
		return nil, err
	}

	fileInfo, ok := result.(fs.FileInfo)
	if !ok {
		return nil, errors.New("invalid file info type")
	}

	return fileInfo, nil
}

func (m *BasicSFTPManager) Open(path string) (*sftp.File, error) {
	result, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return client.Open(path)
	})
	if err != nil {
		return nil, err
	}

	file, ok := result.(sftp.File)
	if !ok {
		return nil, errors.New("invalid file")
	}
	return &file, err
}

func (m *BasicSFTPManager) Rename(oldname, newname string) error {
	_, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return nil, client.Rename(oldname, newname)
	})
	return err
}

func (m *BasicSFTPManager) Create(path string) (*sftp.File, error) {
	result, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return client.Create(path)
	})
	if err != nil {
		return nil, err
	}

	file, ok := result.(sftp.File)
	if !ok {
		return nil, errors.New("invalid file")
	}
	return &file, err
}

func (m *BasicSFTPManager) ReadDir(p string) ([]fs.FileInfo, error) {
	result, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return client.ReadDir(p)
	})
	if err != nil {
		return nil, err
	}

	fileInfo, ok := result.([]fs.FileInfo)
	if !ok {
		return nil, errors.New("invalid file info type")
	}
	return fileInfo, err
}

func (m *BasicSFTPManager) MkdirAll(path string) error {
	_, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return nil, client.MkdirAll(path)
	})
	return err
}

func (m *BasicSFTPManager) Chown(path string) error {
	_, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return nil, client.Chown(path, 1000, 1000)
	})
	return err
}

func (m *BasicSFTPManager) OpenFile(path string, f int) (*sftp.File, error) {
	result, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return client.OpenFile(path, f)
	})
	file, ok := result.(sftp.File)
	if !ok {
		return nil, errors.New("invalid file")
	}
	return &file, err
}

func (m *BasicSFTPManager) Chmod(path string, mode fs.FileMode) error {
	_, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return nil, client.Chmod(path, mode)
	})
	return err
}

func (m *BasicSFTPManager) Remove(path string) error {
	_, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return nil, client.Remove(path)
	})
	return err
}

func (m *BasicSFTPManager) Mkdir(path string) error {
	_, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return nil, client.Mkdir(path)
	})
	return err
}

func (m *BasicSFTPManager) Lstat(p string) (fs.FileInfo, error) {
	result, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return client.Lstat(p)
	})
	if err != nil {
		return nil, err
	}

	fileInfo, ok := result.(fs.FileInfo)
	if !ok {
		return nil, errors.New("invalid file info type")
	}

	return fileInfo, nil
}

func (m *BasicSFTPManager) RemoveDirectory(path string) error {
	_, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return nil, client.RemoveDirectory(path)
	})
	return err
}

func (m *BasicSFTPManager) Join(elem ...string) (string, error) {
	result, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return client.Join(elem...), nil
	})
	if err != nil {
		return "", err
	}

	joinResult, ok := result.(string)
	if !ok {
		return "", errors.New("invalid join result type")
	}

	return joinResult, err
}

func (m *BasicSFTPManager) Chtimes(path string, atime time.Time, mtime time.Time) error {
	_, err := m.withConnectionCheck(func(client *sftp.Client) (interface{}, error) {
		return nil, client.Chtimes(path, atime, mtime)
	})
	return err
}
