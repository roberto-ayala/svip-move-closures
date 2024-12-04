build:
	- go build .

win:
	- GOOS=windows GOARCH=amd64 go build .

linux:
	- GOOS=linux GOARCH=amd64 go build .