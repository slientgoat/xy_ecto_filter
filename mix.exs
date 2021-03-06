defmodule EctoFilter.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_filter,
      version: "0.0.0",
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:timex, "~> 3.1"},
      {:ecto_sql, "~> 3.0.0"}
    ]
  end
end
