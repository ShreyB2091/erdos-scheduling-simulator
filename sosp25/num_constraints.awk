# Extract numConstraints and compute average
BEGIN {
    total = 0
    count = 0
}

{
    # Use regex to extract numConstraints value
    if (match($0, /numConstraints=([0-9]+)/, arr)) {
        constraints = arr[1]
        total += constraints
        count++
        
        # Print the extracted value from each line
        print "Found numConstraints:", constraints
    }
}

END {
    # Calculate and print the average
    if (count > 0) {
        avg = total / count
        print "Total numConstraints:", total
        print "Count of log lines:", count
        print "Average numConstraints:", avg
    } else {
        print "No matching log lines found"
    }
}
