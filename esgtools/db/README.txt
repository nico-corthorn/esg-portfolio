To replicate database from scratch
1. Create a PostgreSQL database in AWS RDS. Make sure to give the database a name and to give public access.
2. Download PostgreSQL in your machine.
	Edit file pg_hba.conf
		# IPv4 local connections:
		host    all             all             127.0.0.1/32            md5
		host    all             all             0.0.0.0/0               md5
	Edit postgresql.conf
		listen_addresses = '*'
3. Connect to remote db by creating a new server in your local PostgreSQL. Give it a name, and then in Connection copy in Host name/address the connection string available in AWS (Punto de enlace)
4. Run the queries to create tables in this folder in your local PostgreSQL client (now connected to the remote AWS db)
