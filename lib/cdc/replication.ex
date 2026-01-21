# defmodule CDC.Replication do
#   use Postgrex.ReplicationConnection

#   alias CDC.Protocol
#   alias Shirath.{RawQueryMapper, Repo, TableCopy}

#   require Logger

#   defstruct [
#     :publications,
#     :protocol,
#     :slot,
#     :state,
#     subscribers: %{}
#   ]

#   def start_link(opts) do
#     conn_opts = [auto_reconnect: true]
#     publications = opts[:publications] || raise ArgumentError, message: "`:publications` missing"
#     slot = opts[:slot] || raise ArgumentError, message: "`:slot` missing"

#     Postgrex.ReplicationConnection.start_link(
#       __MODULE__,
#       {slot, publications},
#       conn_opts ++ opts
#     )
#   end

#   def subscribe(pid, opts \\ []) do
#     Postgrex.ReplicationConnection.call(pid, :subscribe, Keyword.get(opts, :timeout, 5_000))
#   end

#   def unsubscribe(pid, ref, opts \\ []) do
#     Postgrex.ReplicationConnection.call(
#       pid,
#       {:unsubscribe, ref},
#       Keyword.get(opts, :timeout, 5_000)
#     )
#   end

#   @impl true
#   def init({slot, pubs}) do
#     {:ok,
#      %__MODULE__{
#        slot: slot,
#        publications: pubs,
#        protocol: Protocol.new()
#      }}
#   end

#   @impl true
#   def handle_connect(%__MODULE__{slot: slot} = state) do
#     check_conn = Repo.query!("select slot_name, restart_lsn::text from pg_replication_slots where slot_name = '#{slot}'") |> RawQueryMapper.get() |> List.first()
#     {state_status, query} = if check_conn do
#       {:continue_slot, "select * from pg_replication_slots where slot_name = '#{slot}'"}
#     else
#       {:create_slot, "CREATE_REPLICATION_SLOT #{slot} LOGICAL pgoutput EXPORT_SNAPSHOT"}
#     end

#     # query = "CREATE_REPLICATION_SLOT #{slot} LOGICAL pgoutput EXPORT_SNAPSHOT"

#     Logger.debug("[create slot] query=#{query}")

#     {:query, query, %{state | state: state_status}}
#   end

#   @impl true
#   def handle_result(
#         [%Postgrex.Result{} = x_result | _],
#         %__MODULE__{state: state_status, publications: pubs} = state
#       ) do
#     opts = [proto_version: 1, publication_names: pubs]
#     x_result = Shirath.RawQueryMapper.get(x_result) |> List.first()
#     lsn = if state_status == :create_slot do
#       Task.start(fn ->
#         TableCopy.run()
#       end)
#       "0/0"
#     else
#       x_result.restart_lsn
#     end
#     query = "START_REPLICATION SLOT #{state.slot} LOGICAL #{lsn} #{escape_options(opts)}"

#     Logger.debug("[start streaming] query=#{query}")
#     {:stream, query, [], %{state | state: :streaming}}
#   end

#   # defp set_standby() do
#   #   Process.sleep(10_000_000)
#   # end

#   @impl true
#   def handle_data(msg, state) do
#     {return_msgs, tx, protocol} = Protocol.handle_message(msg, state.protocol, state.publications)

#     if not is_nil(tx) do
#       notify(tx, state.subscribers)
#     end

#     {:noreply, return_msgs, %{state | protocol: protocol}}
#   end

#   # Replies must be sent using `reply/2`
#   # https://hexdocs.pm/postgrex/Postgrex.ReplicationConnection.html#reply/2
#   @impl true
#   def handle_call(:subscribe, {pid, _} = from, state) do
#     ref = Process.monitor(pid)

#     state = put_in(state.subscribers[ref], pid)

#     Postgrex.ReplicationConnection.reply(from, {:ok, ref})

#     {:noreply, state}
#   end

#   def handle_call({:unsubscribe, ref}, from, state) do
#     {reply, new_state} =
#       case state.subscribers do
#         %{^ref => _pid} ->
#           Process.demonitor(ref, [:flush])

#           {_, state} = pop_in(state.subscribers[ref])
#           {:ok, state}

#         _ ->
#           {:error, state}
#       end

#     from && Postgrex.ReplicationConnection.reply(from, reply)

#     {:noreply, new_state}
#   end

#   @impl true
#   def handle_info({:DOWN, ref, :process, _, _}, state) do
#     handle_call({:unsubscribe, ref}, nil, state)
#   end

#   defp notify(tx, subscribers) do
#     for {ref, pid} <- subscribers do
#       send(pid, {:notification, self(), ref, tx})
#     end

#     :ok
#   end

#   defp escape_options([]),
#     do: ""

#   defp escape_options(opts) do
#     parts =
#       Enum.map_intersperse(opts, ", ", fn {k, v} -> [Atom.to_string(k), ?\s, escape_string(v)] end)

#     [?\s, ?(, parts, ?)]
#   end

#   defp escape_string(value) do
#     [?', :binary.replace(to_string(value), "'", "''", [:global]), ?']
#   end
# end
