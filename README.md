🧪 Project Purpose
The goal of this Proof of Concept is to validate a secure and scalable data ingestion pattern. It focuses on the technical feasibility of moving large-scale files (500MB+) across three distinct environments (SFTP, Cloud Storage, and On-premise Database) using Apache Airflow 3.1.0.

🎯 Validated Objectives
Cross-Container Connectivity: Proven stable communication between Airflow, SQL Server, and SFTP containers using Docker's internal networking.

Driver Compatibility: Successful implementation of Microsoft ODBC Driver 18 on a Debian-based Airflow 3 image, overcoming unixODBC dependency hurdles.

Large File Handling: Validated a memory-efficient "Download & Chunk" strategy, ensuring 500MB+ files are processed without crashing worker containers.

Provider Integration: Tested and confirmed the interoperability of apache-airflow-providers-amazon, sftp, and odbc in a modern Airflow 3 environment.

🛠️ Technical Challenges Solved
During this POC, several critical blockers were resolved:

Dependency Hell: Fixed library conflicts between pymssql and setuptools_scm by shifting to a pure pyodbc implementation.

Network Handshakes: Resolved Login Timeout and Network Unreachable errors by fine-tuning Docker extra_hosts and gateway IPs.

SSL/TLS Protocol: Adjusted Driver 18 encryption settings (Encrypt=no / TrustServerCertificate=yes) to work with local SQL Server instances.

📊 POC Results
SFTP -> S3: Verified file integrity after transfer.

S3 -> SQL Server: Successfully ingested 500,000+ rows (500MB) using Pandas chunking with a stable memory footprint.
