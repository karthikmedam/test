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

// Convert the threshold time to Unix epoch time
long thresholdTimeInSeconds = thresholdTime.getEpochSecond()

// Construct the command
String command = "find ${directoryPath} -type f -newermt \"$(date -r ${thresholdTimeInSeconds} +'%Y%m%d%H%M.%S')\" -delete"

// Create a ProcessBuilder instance
def processBuilder = new ProcessBuilder(command.split(" "))

// Start the process
Process process = processBuilder.start()

// ... (Rest of the code remains the same)
