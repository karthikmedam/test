import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.temporal.ChronoUnit

// Define the directory path
String directoryPath = "/path/to/directory"

// Define the age threshold in minutes
int ageThreshold = 5

// Get the current time
Instant currentTime = Instant.now()

// Calculate the threshold time
Instant thresholdTime = currentTime.minus(ageThreshold, ChronoUnit.MINUTES)

// Construct the command
String command = "find ${directoryPath} -type f -newermt '${thresholdTime.toString()}' -delete"

// Create a ProcessBuilder instance
def processBuilder = new ProcessBuilder(command.split(" "))

// Start the process
Process process = processBuilder.start()

// Get the output streams
InputStream inputStream = process.getInputStream()
InputStream errorStream = process.getErrorStream()

// Read and print the output
inputStream.eachLine { line ->
    println line
}

// Read and print the error stream (if any)
errorStream.eachLine { line ->
    println "Error: $line"
}

// Wait for the process to complete
int exitCode = process.waitFor()

// Print the exit code
println "Exit code: $exitCode"
