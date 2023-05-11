module OCLog

#using JSON3
#using Pipe: @pipe
using SQLite
using DataFrames
using Sockets
using Genie

# The IP address you want to bind on can be specified as the first argument,
# otherwise it will default to all interfaces.
local_address = isempty(ARGS) ? ip"0.0.0.0" : IPv4(first(ARGS))

println("Starting log collector...")
atexit(() -> println("Log collector fully shutdown."))

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
port = 5140 # Remember that Linux needs root to open ports <1024.
group = ip"239.5.1.4"
bind(socket, local_address, port) || throw("Failed to open port $(port)")
join_multicast_group(socket, group)
# Close the socket and leave the multicast group upon closure.
# Note that these are in reverse order due to the LIFO semantics of atexit.
atexit(() -> close(socket))
atexit(() -> leave_multicast_group(socket, group))

# We are now ready to actually receive the logs.
# First, we need a prepared statement that we will use to safely insert values.
insert_statement = SQLite.Stmt(db, "INSERT INTO Logs (Message) VALUES (?)")
atexit(() -> DBInterface.close!(insert_statement))
atexit(() -> println("Shutting down log collector."))

route("/") do
    "Genie is running. See /txt and /json."
end

route("/txt") do
    df = DBInterface.execute(db, "SELECT * FROM Logs") |> DataFrame
    respond(join(df.Message, "\n"), :text)
end

using Genie.Renderer.Json
route("/json") do
    DBInterface.execute(db, "SELECT * FROM Logs") |> DataFrame |> json
end

up(8888, "0.0.0.0") # we need Genie to bind to all interfaces.
atexit(down)

# Read messages as they come in and commit them to the database.
while true
    bmessage = recv(socket)
    isempty(bmessage) && break
    Char(last(bmessage)) != '\n' && println(stderr, "This log message should have ended in a newline (actual byte read is $(last(bmessage)))")
    pri = bmessage[1:5] # Look in the RFC for what this means. For me, I don't need it.
    message = String(bmessage[6:end-1])
    #println(message)
    DBInterface.execute(insert_statement, (message,))
end

end # module
