using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using Cave.Collections.Generic;
using Cave.Data.Sql;

namespace Cave.Data
{
    class SqlConnectionPool
    {
        #region Private Fields

        readonly LinkedList<SqlConnection> Queue = new LinkedList<SqlConnection>();
        readonly SqlStorage Storage;
        readonly Set<SqlConnection> Used = new Set<SqlConnection>();
        TimeSpan? timeout = TimeSpan.FromMinutes(5);

        #endregion Private Fields

        #region Private Methods

        SqlConnection GetQueuedConnection(string database)
        {
            var nextNode = Queue.First;
            LinkedListNode<SqlConnection> selectedNode = null;
            while (nextNode != null)
            {
                // get current and next node
                var currentNode = nextNode;
                nextNode = currentNode.Next;

                // remove dead and old connections
                if ((currentNode.Value.State != ConnectionState.Open) || (DateTime.UtcNow > (currentNode.Value.LastUsed + timeout.Value)))
                {
                    Trace.TraceInformation($"Closing connection {currentNode.Value} (livetime exceeded) (Idle:{Queue.Count} Used:{Used.Count})");
                    currentNode.Value.Dispose();
                    Queue.Remove(currentNode);
                    continue;
                }

                // allow only connection with matching db name ?
                if (!Storage.DBConnectionCanChangeDataBase)
                {
                    // check if database name matches
                    if (currentNode.Value.Database != database)
                    {
                        continue;
                    }
                }

                // set selected node
                selectedNode = currentNode;

                // break if we found a perfect match
                if (currentNode.Value.Database == database)
                {
                    break;
                }
            }

            if (selectedNode != null)
            {
                // if we got a connection bound to a specific database but need an unbound, we have to create a new one.
                if ((database == null) && (selectedNode.Value.Database != null))
                {
                    return null;
                }

                // we got a matching connection, remove node
                Queue.Remove(selectedNode);
                Used.Add(selectedNode.Value);
                return selectedNode.Value;
            }

            // nothing found
            return null;
        }

        #endregion Private Methods

        #region Public Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlConnectionPool"/> class.
        /// </summary>
        /// <param name="storage">The storage.</param>
        public SqlConnectionPool(SqlStorage storage) => this.Storage = storage;

        #endregion Public Constructors

        #region Public Properties

        /// <summary>
        /// Gets or sets the connection close timeout.
        /// </summary>
        /// <value>The connection close timeout.</value>
        public TimeSpan ConnectionCloseTimeout { get => timeout.Value; set => timeout = value; }

        #endregion Public Properties

        #region Public Methods

        /// <summary>
        /// Clears the whole connection pool (forced, including connections in use).
        /// </summary>
        public void Clear()
        {
            lock (this)
            {
                foreach (var connection in Used)
                {
                    Trace.TraceInformation($"Closing connection {connection} (pool clearing)");
                    connection.Close();
                }

                foreach (var connection in Queue)
                {
                    Trace.TraceInformation($"Closing connection {connection} (pool clearing)");
                    connection.Close();
                }

                Queue.Clear();
                Used.Clear();
            }
        }

        /// <summary>
        /// Closes this instance.
        /// </summary>
        public void Close() => Clear();

        /// <summary>
        /// Gets the connection.
        /// </summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <returns>A new or free <see cref="SqlConnection"/> instance.</returns>
        public SqlConnection GetConnection(string databaseName)
        {
            lock (this)
            {
                var connection = GetQueuedConnection(databaseName);
                if (connection == null)
                {
                    Trace.TraceInformation("Creating new connection for Database {0} (Idle:{1} Used:{2})", databaseName, Queue.Count, Used.Count);
                    var iDbConnection = Storage.CreateNewConnection(databaseName);
                    connection = new SqlConnection(databaseName, iDbConnection);
                    Used.Add(connection);
                    Trace.TraceInformation($"Created new connection for Database {databaseName} (Idle:{Queue.Count} Used:{Used.Count})");
                }
                else
                {
                    if (connection.Database != databaseName)
                    {
                        connection.ChangeDatabase(databaseName);
                    }
                }

                return connection;
            }
        }

        /// <summary>
        /// Returns a connection to the connection pool for reuse.
        /// </summary>
        /// <param name="connection">The connection to return to the queue.</param>
        /// <param name="close">Force close of the connection.</param>
        public void ReturnConnection(ref SqlConnection connection, bool close = false)
        {
            if (connection == null)
            {
                throw new ArgumentNullException(nameof(connection));
            }

            lock (this)
            {
                if (Used.Contains(connection))
                {
                    Used.Remove(connection);
                    if (!close && (connection.State == ConnectionState.Open))
                    {
                        Queue.AddFirst(connection);
                        connection = null;
                        return;
                    }
                }
            }

            Trace.TraceInformation($"Closing connection {connection} (sql error)");
            connection.Close();
            connection = null;
        }

        public override string ToString()
        {
            lock (this)
            {
                return $"SqlConnectionPool {Storage} queue:{Queue.Count} used:{Used.Count}";
            }
        }

        #endregion Public Methods
    }
}
