build:
	- go build -o bin/move-docs-mac .

win:
	- GOOS=windows GOARCH=amd64 go build -o bin/move-docs-win .

linux:
	- GOOS=linux GOARCH=amd64 go build -o bin/move-docs-linux .