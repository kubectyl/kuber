package filesystem

import (
	"time"
)

// Returns the time that the file/folder was created.
func (s *Stat) CTime() time.Time {
	// st := s.Sys().(*sftp.FileStat)

	// fmt.Println("Atime: ", st.Atime)

	// Do not remove these "redundant" type-casts, they are required for 32-bit builds to work.
	// return time.Unix(int64(st.Ctim.Sec), int64(st.Ctim.Nsec))
	return time.Now()
}
