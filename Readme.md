# Scalable, Fault Tolerant File Management System

This GitHub repository utilizes the RAFT protocol to enhance server resilience when facilitating client communication. The mechanism for file storage involves partitioning files into segments and associating them with hash values. For download operations, the repository employs the consistent hashing algorithm to pinpoint the pertinent segments.
