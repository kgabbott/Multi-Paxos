# Multi-Paxos
This is a simple Multi-Paxos implementation that I built as a final project for CPSC 625. It is built as a replicted log 
utilizing Multi-Paxos to maintain consistency between replicas. On top of the replicated log, I built a simple key-value store,
where the entries in the log are client requests on the Key-Value database. A report on the system can be found [here](https://github.com/kgabbott/Multi-Paxos/blob/master/Final_Project_Report.pdf).

In order to run the system, first change `settings/perrnames*.csv` to have the names and ip addresses of the 3 or 5 server nodes, where `*` is 3 or 5.

Then, on each of the server nodes change into the server directory and run `python paxos_main.py`.

Finally, on a client node change into the client directory and run `python client_main.py`.
