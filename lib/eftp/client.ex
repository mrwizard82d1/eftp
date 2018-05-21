#
# Copyright 2017, Audian, Inc. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

defmodule Eftp.Client do
  @moduledoc """
  Ftp Client Functions
  """

  require Logger

  @doc """
  Connects to an FTP Server. If successful, returns a PID. This pid will be
  passed to the authenticate command

  ## Examples
  ```elixir
  iex> Eftp.Client.connect("ftp.example.net", 21)
  #PID<0.158.0>
  ```
  """
  def connect(host, port \\ 21) do
    case :inets.start(:ftpc, host: '#{host}', port: '#{port}', progress: true, verbose: true, timeout: 3600000) do
      {:ok, pid} ->
        pid
      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Disconnects from an FTP Server. If successful, returns term `{:ok, "QUIT"}. If already disconnected, returns `:ok`.

  ## Examples
  ```elixir
  iex> Eftp.Client.disconnect(#PID<0.158.0>, 21)
  ```elixir
  {:ok, "QUIT"}

  OR

  :ok

  """
  def disconnect(pid) do
    :inets.stop(:ftpc, pid)
  end

  @doc """
  Authenticate against an ftp server. If successful, returns a PID. This pid will be
  passed to the fetch commands

  ## Examples
  ```elixir
  iex> Eftp.Client.authenticate("foo", "bar")
  ```elixir
  #PID<0.158.0>

  OR

  {:error, :invalid_auth}
  """
  def authenticate(pid, username, password) do
    case :ftp.user(pid, '#{username}', '#{password}') do
      :ok -> pid
      {:error, :euser} -> {:error, :invalid_auth}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Fetch a specific file from the server. A list of file names can also be passed and
  each file will be downloaded
  """
  def fetch(pid, remote_filename, local_directory) when is_binary(remote_filename) do
    case pid do
      {:error, reason} -> {:error, reason}
      _ ->
        dir = Path.dirname(remote_filename)
        filename = Path.basename(remote_filename)
        local_filename = "#{local_directory}/#{filename}"

        case File.exists?(local_filename) do
          false ->
            :ftp.cd(pid, '#{dir}')
            :ftp.type(pid, :binary)
            case :ftp.recv(pid, '#{filename}', '#{local_filename}') do
              :ok -> :ok
              {:error, reason} ->
                File.rm(local_filename)
                {:error, reason}
            end
          true ->
            File.rename(local_filename, "#{local_filename}-#{unixtime()}.backup")
            fetch(pid, remote_filename, local_directory)
        end
    end
  end

  @doc """
  Fetch list of files from the server
  """
  def fetch(pid, files, local_directory) when is_list(files) do
    case pid do
      {:error, reason} -> {:error, reason}
      _ ->
        for file <- files do
          case fetch(pid, "#{file}", local_directory) do
            :ok -> :ok
            {:error, reason} ->
              {:error, reason}
          end
        end
    end
  end

  @doc """
  Downloads a remote file into a local directory using chunks.
  """
  def download(pid, remote_file, local_directory) do
    Logger.info("Eftp.Client.download called with")
    Logger.info("  pid: #{inspect pid}")
    Logger.info("  remote file path: #{remote_file}")
    Logger.info("  local directory: #{local_directory}")

    :ok = :ftp.recv_chunk_start(pid, '#{remote_file}')
    local_file_name = Path.join(local_directory, remote_file)
    local_dir = Path.dirname(local_file_name)
    :ok = File.mkdir_p(local_dir)
    {:ok, local_file} = File.open(local_file_name, [:binary, :append])
    {:ok, bytes_transferred} = append(pid, remote_file, local_file, 0)
    File.close(local_file)
    Logger.info("Successfully transferred #{bytes_transferred} bytes.")
    {:ok, bytes_transferred}
  end

  @doc """
  Appends data from remote file to local file.
  """
  def append(pid, remote_file, local_file, so_far) do
    case :ftp.recv_chunk(pid) do
      :ok -> {:ok, so_far}
      {:ok, chunk} ->
        IO.write(local_file, chunk)
        Logger.info("Transferred #{byte_size(chunk)} bytes.")
        :ftp.pwd(pid) # send a simple command to keep control connection alive
#        :ftp.quote(pid, 'NOOP')
#        Logger.info("Sent NOOP")
        append(pid, remote_file, local_file, so_far + byte_size(chunk))
      {:error, :trans_neg_compl} ->
        Logger.info("Transient error. Retrying.")
        append(pid, remote_file, local_file, so_far)
      {:error, reason} ->
        detailed_error = :ftp.formaterror(reason)
        Logger.info("Append failed: #{detailed_error}")
        {:error, reason}
    end
  end

  @doc """
  Retrieves list of files from the current directory
  """
  def list(pid) do
    {:ok, files} = :ftp.nlist(pid)

    files
    |> List.to_string
    |> String.split("\r\n")
    |> Enum.reject(fn(x) -> x == "" end)
  end

  #-- PRIVATE --#
  defp unixtime() do
    :os.system_time(:seconds)
  end
end
