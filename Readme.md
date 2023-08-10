# Scalable, Fault Tolerant File Management System

This GitHub repository utilizes the RAFT protocol to enhance server resilience when facilitating client communication. The mechanism for file storage involves partitioning files into segments and associating them with hash values. For download operations, the repository employs the consistent hashing algorithm to pinpoint the pertinent segments.

## Examples:

1.
```shell
Run the commands below on separate terminals (or nodes)
> go run cmd/SurfstoreServerExec/main.go -s block -p 8081 -l
> go run cmd/SurfstoreServerExec/main.go -s block -p 8082 -l
> go run cmd/SurfstoreServerExec/main.go -s meta -l localhost:8081 localhost:8082
```
The first two lines start two servers that services BlockStore interface and listens to localhost on port 8081 and 8082. The third line starts a server that services MetaStore interface, listens to localhost on port 8080, and references the BlockStore we created as the underlying BlockStore. (Note: if these are on separate nodes, then you should use the public ip address and remove `-l`)

2. From a new terminal (or a new node), run the client using the script provided in the starter code (if using a new node, build using step 1 first). Use a base directory with some files in it.
```shell
> mkdir dataA
> cp ~/pic.jpg dataA/ 
> go run cmd/SurfstoreClientExec/main.go localhost:8080 dataA 4096
```
This would sync pic.jpg to the server hosted on localhost:8080, using `dataA` as the base directory, with a block size of 4096 bytes.

3. From another terminal (or a new node), run PrintBlockMapping to check which blocks a block server has. 
```shell
> go run cmd/SurfstorePrintBlockMapping/main.go localhost:8080 dataB 4096
```
The output willl be a map from block hashes to server names. 
