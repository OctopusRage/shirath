# defmodule CDC.Protocol do
#   import Postgrex.PgOutput.Messages
#   require Logger

#   alias CDC.Tx
#   alias Postgrex.PgOutput.Lsn

#   @type t :: %__MODULE__{
#           tx: term(),
#           relations: map()
#         }

#   defstruct [
#     :tx,
#     relations: %{}
#   ]

#   @spec new() :: t()
#   def new do
#     %__MODULE__{}
#   end

#   def handle_message(msg, state, pub) when is_binary(msg) do
#     msg
#     |> decode()
#     |> handle_message(state, pub)
#   end

#   def handle_message(msg_primary_keep_alive(reply: 0), state, _) do
#     {[], nil, state}
#   end

#   def handle_message(msg_primary_keep_alive(server_wal: lsn, reply: 1), state, _) do
#     <<lsn::64>> = Lsn.encode(lsn)

#     {[standby_status_update(lsn)], nil, state}
#   end

#   def handle_message(msg, %__MODULE__{tx: nil, relations: relations} = state, _pub) do
#     tx =
#       [relations: relations, decode: true]
#       |> Tx.new()
#       |> Tx.build(msg)


#     {[], nil, %{state | tx: tx}}
#   end

#   def handle_message(msg, %__MODULE__{tx: tx} = state, _pub) do
#     case Tx.build(tx, msg) do
#       %Tx{state: :commit, relations: relations} ->
#         tx = Tx.finalize(tx)
#         relations = Map.merge(state.relations, relations)
#         if tx.relations |> Enum.count() > 0 do
#           Task.start(fn ->
#             Shirath.Ingestor.push_message(tx)
#           end)
#         end


#         {[], tx, %{state | tx: nil, relations: relations}}

#       tx ->
#         {[], nil, %{state | tx: tx}}
#     end
#   end


#   defp standby_status_update(lsn) do
#     [
#       wal_recv: lsn + 1,
#       wal_flush: lsn + 1,
#       wal_apply: lsn + 1,
#       system_clock: now(),
#       reply: 0
#     ]
#     |> msg_standby_status_update()
#     |> encode()
#   end
# end
