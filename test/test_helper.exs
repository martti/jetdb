require Logger

defmodule TestData do
  def check_data() do
    if File.exists?('testdata/data/nwind.mdb') do
      :ok
    else
      {:error, :nofile}
    end
  end

  def download_data() do
    if System.cmd("git", ["clone", "https://github.com/mdbtools/mdbtestdata.git", "testdata"]) do
      :ok
    else
      {:error, :nofile}
    end
  end
end

case TestData.check_data() do
  {:error, _} ->
    Logger.warn("Test data does not exist. Downloading...")

    case TestData.download_data() do
      {:error, error} ->
        Logger.warn(["Download failed with", inspect(error, prettty: true)])

      :ok ->
        :ok
    end

  :ok ->
    :ok
end

ExUnit.start()
