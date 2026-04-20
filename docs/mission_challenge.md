# Space Mission Log Analysis Challenge

## Background
You are a data analyst at the Interplanetary Space Agency. Your task is to analyze a complex log file containing data about thousands of space missions conducted between 2030 and 2070. We want to find the security code of the **longest successful Mars mission** in the database.

## Instructions
1. You are provided with a file called `space_missions.log`
2. The file contains information about space missions with fields separated by '|' characters
3. Fields: Date | Mission ID | Destination | Status | Crew Size | Duration (days) | Success Rate | Security Code
4. Use awk to analyze this file and find the mission that meets the criteria
5. Submit the security code as your answer (format: ABC-123-XYZ)

## Tips
- The file contains commented lines (starting with #) and other irrelevant data
- Field separators may have inconsistent spacing
- Only consider missions with "Completed" status
- "Duration" is measured in days

## Example Line
```
2045-07-12 | KLM-1234 | Mars | Completed | 5 | 387 | 98.7 | TRX-842-YHG
```

Good luck, analyst!

