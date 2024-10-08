const fs = require('fs');
const path = require('path');

// Create log directory if it doesn't exist
const logDirectory = path.join(__dirname, 'logs');
if (!fs.existsSync(logDirectory)) {
    fs.mkdirSync(logDirectory);
}

// Function to log messages
const logMessage = (filename, message) => {
    const filePath = path.join(logDirectory, filename);
    const logEntry = `${new Date().toISOString()} - ${message}\n`;
    fs.appendFile(filePath, logEntry, (err) => {
        if (err) throw err;
    });
};

module.exports = {
    logSuccess: (message) => logMessage('success.log', message),
    logError: (message) => logMessage('error.log', message),
};
