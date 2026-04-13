job "hello-world" {
  type = "batch"

  group "app" {
    count = 1

    task "hello" {
      driver = "raw_exec"

      config {
        command = "powershell.exe"
        args = [
          "-NoProfile",
          "-Command",
          "Write-Output 'Hello, world from Nomad!'"
        ]
      }

      resources {
        cpu    = 1
        memory = 64
      }
    }
  }
}