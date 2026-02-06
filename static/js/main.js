let currentFilename = null;
let currentZipFile = null;

// Drag and drop functionality
const uploadArea = document.getElementById('uploadArea');
const fileInput = document.getElementById('fileInput');

uploadArea.addEventListener('dragover', (e) => {
    e.preventDefault();
    uploadArea.style.borderColor = '#764ba2';
    uploadArea.style.background = '#f0f4ff';
});

uploadArea.addEventListener('dragleave', (e) => {
    e.preventDefault();
    uploadArea.style.borderColor = '#667eea';
    uploadArea.style.background = 'white';
});

uploadArea.addEventListener('drop', (e) => {
    e.preventDefault();
    uploadArea.style.borderColor = '#667eea';
    uploadArea.style.background = 'white';
    
    const files = e.dataTransfer.files;
    if (files.length > 0) {
        handleFile(files[0]);
    }
});

fileInput.addEventListener('change', (e) => {
    if (e.target.files.length > 0) {
        handleFile(e.target.files[0]);
    }
});

function handleFile(file) {
    if (!file.name.match(/\.(xlsx|xls|csv)$/i)) {
        showError('Please upload an Excel (.xlsx, .xls) or CSV (.csv) file.');
        return;
    }
    
    uploadFile(file);
}

function uploadFile(file) {
    const formData = new FormData();
    formData.append('file', file);
    
    showProgress('Uploading file...', 0);
    
    fetch('/upload', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (data.error) {
            showError(data.error);
            return;
        }
        
        currentFilename = data.filename;
        
        // Update file info display
        document.getElementById('fileInfo').style.display = 'block';
        document.getElementById('fileName').textContent = data.filename;
        document.getElementById('fileRows').textContent = data.row_count.toLocaleString();
        document.getElementById('fileColumns').textContent = data.columns.length;
        
        // Enable process button
        document.getElementById('processBtn').disabled = false;
        
        hideProgress();
        
        // Show success animation
        uploadArea.innerHTML = `
            <i class="fas fa-check-circle upload-icon" style="color: #28a745;"></i>
            <p>File uploaded successfully!</p>
            <p><strong>${data.filename}</strong></p>
            <p>${data.row_count.toLocaleString()} rows, ${data.columns.length} columns</p>
        `;
        
        uploadArea.style.borderColor = '#28a745';
        uploadArea.style.background = '#f0f4ff';
    })
    .catch(error => {
        showError('Error uploading file: ' + error.message);
        hideProgress();
    });
}

// Process file
document.getElementById('processBtn').addEventListener('click', () => {
    if (!currentFilename) {
        showError('Please upload a file first.');
        return;
    }
    
    showProgress('Processing file... This may take a moment...', 10);
    
    fetch('/process', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ filename: currentFilename })
    })
    .then(response => response.json())
    .then(data => {
        if (data.error) {
            showError(data.error);
            hideProgress();
            return;
        }
        
        // Update progress
        updateProgress(50, 'Creating Excel files...');
        
        setTimeout(() => {
            updateProgress(80, 'Creating ZIP archive...');
            
            setTimeout(() => {
                updateProgress(100, 'Processing complete!');
                
                currentZipFile = data.zip_file;
                
                // Update output count
                document.getElementById('outputCount').textContent = data.files_created;
                
                // Show download section
                document.getElementById('downloadSection').style.display = 'block';
                document.getElementById('downloadInfo').textContent = 
                    `Created ${data.files_created} Excel files in the ZIP archive.`;
                
                // Scroll to download section
                document.getElementById('downloadSection').scrollIntoView({ behavior: 'smooth' });
                
                hideProgress();
            }, 1000);
        }, 1000);
    })
    .catch(error => {
        showError('Error processing file: ' + error.message);
        hideProgress();
    });
});

// Download file
document.getElementById('downloadBtn').addEventListener('click', () => {
    if (!currentZipFile) {
        showError('No file available for download.');
        return;
    }
    
    // Create download link
    const downloadUrl = `/download/${currentZipFile}`;
    
    // Create temporary anchor element
    const a = document.createElement('a');
    a.href = downloadUrl;
    a.download = currentZipFile;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    
    // Add pulse animation to button
    const btn = document.getElementById('downloadBtn');
    btn.classList.add('pulse');
    
    setTimeout(() => {
        btn.classList.remove('pulse');
    }, 2000);
});

// Progress functions
function showProgress(text, width) {
    const container = document.getElementById('progressContainer');
    const bar = document.getElementById('progressBar');
    const textEl = document.getElementById('progressText');
    
    container.style.display = 'block';
    bar.style.width = `${width}%`;
    textEl.textContent = text;
}

function updateProgress(width, text) {
    const bar = document.getElementById('progressBar');
    const textEl = document.getElementById('progressText');
    
    bar.style.width = `${width}%`;
    if (text) {
        textEl.textContent = text;
    }
}

function hideProgress() {
    const container = document.getElementById('progressContainer');
    setTimeout(() => {
        container.style.display = 'none';
    }, 1000);
}

// Error handling
function showError(message) {
    const errorSection = document.getElementById('errorSection');
    const errorMessage = document.getElementById('errorMessage');
    
    errorMessage.textContent = message;
    errorSection.style.display = 'block';
    
    // Scroll to error
    errorSection.scrollIntoView({ behavior: 'smooth' });
}

function hideError() {
    const errorSection = document.getElementById('errorSection');
    errorSection.style.display = 'none';
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    // Add event listener for cleanup (optional)
    window.addEventListener('beforeunload', () => {
        fetch('/cleanup', { method: 'POST' })
            .catch(error => console.error('Cleanup error:', error));
    });
});