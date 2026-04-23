job "hello-world" {
  type = "batch"

  group "app" {
    count = 1

    task "hello" {
      driver = "raw_exec"

      config {
        command = "C:\\Windows\\System32\\cmd.exe"
        args = [
          "/c",
          "echo Hello, world from Nomad!"
        ]
      }

      resources {
        cpu    = 1
        memory = 64
      }
    }
  }
}