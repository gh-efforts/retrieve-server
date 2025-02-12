package main

import "github.com/service-sdk/go-sdk-qn/v2/operation"

type DownloadReaderAt struct {
	downloader *operation.Downloader
	key        string
}

func NewDownloadReaderAt(key string) *DownloadReaderAt {
	return &DownloadReaderAt{
		downloader: operation.NewDownloaderV2(),
		key:        key,
	}
}

func (d *DownloadReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	size := int64(len(p))
	_, data, err := d.downloader.DownloadRangeBytes(d.key, off, size)
	if err != nil {
		return 0, err
	}
	return copy(p, data), nil
}
