package journal

import (
	"os"
	"path/filepath"
)

// writeFileAtomic 以 tmp + fsync + rename + fsync(父目录) 的方式原子落盘，
// 崩溃只会留下旧文件或新文件，不会出现写了一半的检查点/快照。
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		return err
	}
	file, err := os.OpenFile(temporary, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	return fsyncDir(dir)
}

// fsyncDir 确保 rename 在目录项层面落盘（崩溃掉电后仍可找见新文件）。
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
