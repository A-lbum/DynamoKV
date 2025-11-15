module github.com/llllleeeewwwiis/standalonekv

go 1.21

require (
    github.com/Connor1996/badger v1.5.1-0.20220222053432-2d2cbf472c77
    google.golang.org/grpc v1.63.0
    google.golang.org/protobuf v1.34.0
)

require (
    github.com/gogo/protobuf v1.3.2 // indirect
    github.com/golang/snappy v0.0.4 // indirect (badger)
    golang.org/x/net v0.22.0        // indirect (grpc)
    golang.org/x/sys v0.18.0        // indirect
)
