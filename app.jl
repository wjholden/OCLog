module OCLog

using JSON3
using Pipe: @pipe
using SQLite
using DataFrames
using Sockets

# Open the SQLite database that we will use to store Syslog messages.
# If it does not exist, then create it.
db = SQLite.DB("syslog.db")
DBInterface.execute(db, """
CREATE TABLE IF NOT EXISTS Logs
(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    Message TEXT NOT NULL,
    Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
)
""")
atexit(() -> close(db)) # Close the database when the program terminates.

# Open the UDP socket to receive logs.
socket = Sockets.UDPSocket()
address = ip"0.0.0.0"
port = 514
group = ip"239.5.1.4"
bind(socket, address, port)
join_multicast_group(socket, group)
# Close the socket and leave the multicast group upon closure.
# Note that these are in reverse order due to the LIFO semantics of atexit.
atexit(() -> close(socket))
atexit(() -> leave_multicast_group(socket, group))

# We are now ready to actually receive the logs.
# First, we need a prepared statement that we will use to safely insert values.
insert_statement = SQLite.Stmt(db, "INSERT INTO Logs (Message) VALUES (?)")
atexit(() -> DBInterface.close!(insert_statement))
atexit(() -> println("Shutting down logging app."))

# Read messages as they come in and commit them to the database.
for _ in 1:10
    bmessage = recv(socket)
    Char(last(bmessage)) != '\n' && println(stderr, "This log message should have ended in a newline (actual byte read is $(last(bmessage)))")
    pri = bmessage[1:5]
    message = String(bmessage[6:end-1])
    println(message)
    DBInterface.execute(insert_statement, (message,))
end

# DBInterface.execute(db, "SELECT * FROM Logs") |> DataFrame |> println

end # module
