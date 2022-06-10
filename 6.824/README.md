# Secure Raft: 
## Authors
Shreesha G Bhat CS18B103
Nischith Shadagopan M N CS18B102

### Boilerplate code for Raft taken from MIT 6.824 Distributed Systems Labs
Course website: http://nil.csail.mit.edu/6.824/2020/schedule.html

### Install Golang 
* The only dependency required is golang. This can be installed using the Ubuntu command 
```
sudo apt-get install golang
```

### Instructions to Test Implementation
* The $\texttt{6.824}$ file (even in the VM) contains all the necessary files. 
* Go to $\texttt{6.824/src/raft}$. Open a terminal and run the following command 
```
go test -run 2
```

This command runs the test cases designed at MIT to stress test the Raft implementation. The coded up modified secure Raft implementation passes all the test cases

* As the result of the tests can vary between runs because of non-determinism in thread scheduling, we can run the tests multiple times using the $\texttt{go-test-many.sh}$ script file as follows
```
./go-test-many.sh <num_of_times> <num_of_threads/cores_to_use> 
```

* The modified raft implementation with comments is present in $\texttt{6.824/src/raft/raft.go}$