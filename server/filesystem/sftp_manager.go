package filesystem

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

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
	if s.closed == true {
		return fmt.Errorf("Connection was already closed")
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
	log        *log.Logger
	connString string
	sshConfig  *ssh.ClientConfig
}

// NewBasicSFTPManager returns a BasicSFTPManager
func NewBasicSFTPManager(connString string, config *ssh.ClientConfig) *BasicSFTPManager {
	var buf bytes.Buffer
	manager := &BasicSFTPManager{
		conns:      make([]*SFTPConn, 0),
		connString: connString,
		sshConfig:  config,
		log:        log.New(&buf, "logger: ", log.Lshortfile),
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
		// fmt.Println("Connection closed, reconnecting: ", res)
		conn, err := ssh.Dial("tcp", m.connString, m.sshConfig)
		if err != nil {
			// fmt.Println("Failed to reconnect:" + err.Error())
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
		return nil, fmt.Errorf("failed to dial ssh: %s", err)
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
