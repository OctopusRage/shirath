defmodule Shirath.MixProject do
  use Mix.Project

  def project do
    [
      app: :shirath,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {Shirath.Application, []},
      extra_applications: [:logger, :inets]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
      {:pillar, "~> 0.37.0"},
      # {:ecto_sql, "~> 3.10.1"},
      # {:postgrex, "~> 0.17.1"},
      {:quantum, "~> 3.0"},
      {:jason, "~> 1.4"},
      {:rename, "~> 0.1.0", only: :dev},
      {:sentry, "~> 9.0"},
      {:hackney, "~> 1.19"},
      {:broadway, "~> 1.0"},
      {:postgrex, "~> 0.17.4"},
      {:ecto_sql, "~> 3.11"},
      {:oban, "~> 2.19"},
      {:bandit, "~>1.1"},
      {:igniter, "~> 0.5", only: [:dev]},
      {:cainophile, git: "https://github.com/OctopusRage/cainophile.git", branch: "improvement/typecast"}
    ]
  end
end
