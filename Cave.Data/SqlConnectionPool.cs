﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using Cave.Collections.Generic;
using Cave.Data.Sql;

namespace Cave.Data
{
    class SqlConnectionPool
    {
        readonly LinkedList<SqlConnection> queue = new LinkedList<SqlConnection>();
        readonly SqlStorage storage;
        readonly Set<SqlConnection> used = new Set<SqlConnection>();
        TimeSpan? timeout = TimeSpan.FromMinutes(5);

        /// <summary>Initializes a new instance of the <see cref="SqlConnectionPool" /> class.</summary>
        /// <param name="storage">The storage.</param>
        public SqlConnectionPool(SqlStorage storage) => this.storage = storage;

        /// <summary>Gets or sets the connection close timeout.</summary>
        /// <value>The connection close timeout.</value>
        public TimeSpan ConnectionCloseTimeout { get => timeout.Value; set => timeout = value; }

        public override string ToString()
        {
            lock (this)
            {
                return $"SqlConnectionPool {storage} queue:{queue.Count} used:{used.Count}";
            }
        }

        /// <summary>Clears the whole connection pool (forced, including connections in use).</summary>
        public void Clear()
        {
            lock (this)
            {
                foreach (var connection in used)
                {
                    Trace.TraceInformation($"Closing connection {connection} (pool clearing)");
                    connection.Close();
                }

                foreach (var connection in queue)
                {
                    Trace.TraceInformation($"Closing connection {connection} (pool clearing)");
                    connection.Close();
                }

                queue.Clear();
                used.Clear();
            }
        }

        /// <summary>Gets the connection.</summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <returns>A new or free <see cref="SqlConnection" /> instance.</returns>
        public SqlConnection GetConnection(string databaseName)
        {
            lock (this)
            {
                var connection = GetQueuedConnection(databaseName);
                if (connection == null)
                {
                    Trace.TraceInformation("Creating new connection for Database {0} (Idle:{1} Used:{2})", databaseName, queue.Count, used.Count);
                    var iDbConnection = storage.CreateNewConnection(databaseName);
                    connection = new SqlConnection(databaseName, iDbConnection);
                    used.Add(connection);
                    Trace.TraceInformation($"Created new connection for Database {databaseName} (Idle:{queue.Count} Used:{used.Count})");
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

        /// <summary>Returns a connection to the connection pool for reuse.</summary>
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
                if (used.Contains(connection))
                {
                    used.Remove(connection);
                    if (!close && (connection.State == ConnectionState.Open))
                    {
                        queue.AddFirst(connection);
                        connection = null;
                        return;
                    }
                }
            }

            Trace.TraceInformation($"Closing connection {connection} (sql error)");
            connection.Close();
            connection = null;
        }

        /// <summary>Closes this instance.</summary>
        public void Close() => Clear();

        SqlConnection GetQueuedConnection(string database)
        {
            var nextNode = queue.First;
            LinkedListNode<SqlConnection> selectedNode = null;
            while (nextNode != null)
            {
                // get current and next node
                var currentNode = nextNode;
                nextNode = currentNode.Next;

                // remove dead and old connections
                if ((currentNode.Value.State != ConnectionState.Open) || (DateTime.UtcNow > (currentNode.Value.LastUsed + timeout.Value)))
                {
                    Trace.TraceInformation($"Closing connection {currentNode.Value} (livetime exceeded) (Idle:{queue.Count} Used:{used.Count})");
                    currentNode.Value.Dispose();
                    queue.Remove(currentNode);
                    continue;
                }

                // allow only connection with matching db name ?
                if (!storage.DBConnectionCanChangeDataBase)
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
                queue.Remove(selectedNode);
                used.Add(selectedNode.Value);
                return selectedNode.Value;
            }

            // nothing found
            return null;
        }
    }
}
