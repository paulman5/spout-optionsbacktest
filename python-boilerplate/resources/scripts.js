document.getElementById('csvFile').addEventListener('change', function(e) {
  const file = e.target.files[0];
  if (file) {
    const reader = new FileReader();
    reader.onload = function(event) {
      const csv = event.target.result;
      displayCSV(csv);
    };
    reader.readAsText(file);
  }
});

function displayCSV(csv) {
  const lines = csv.split('\n');
  const headers = lines[0].split(',');
  
  // Find the strike column index
  const strikeIndex = headers.findIndex(h => h.trim().toLowerCase() === 'strike');
  
  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const tbody = document.createElement('tbody');
  
  // Create header row
  const headerRow = document.createElement('tr');
  headers.forEach((header, index) => {
    const th = document.createElement('th');
    th.textContent = header.trim();
    const colClass = getColumnClass(header.trim());
    if (colClass) {
      th.classList.add(colClass + '-header');
    }
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  
  // Create data rows
  for (let i = 1; i < lines.length; i++) {
    if (lines[i].trim() === '') continue;
    
    const row = document.createElement('tr');
    const values = parseCSVLine(lines[i]);
    
    values.forEach((value, index) => {
      const td = document.createElement('td');
      td.textContent = value.trim();
      
      // Apply color classes based on column name
      const colClass = getColumnClass(headers[index].trim());
      if (colClass) {
        td.classList.add(colClass);
      }
      
      row.appendChild(td);
    });
    
    tbody.appendChild(row);
  }
  
  table.appendChild(thead);
  table.appendChild(tbody);
  
  const container = document.getElementById('tableContainer');
  container.innerHTML = '';
  container.appendChild(table);
}

function getColumnClass(colName) {
  const colLower = colName.toLowerCase();
  if (colLower.includes('strike')) {
    return 'strike';
  } else if (colLower.includes('premium')) {
    return 'premium';
  } else if (colLower.includes('volume')) {
    return 'volume';
  } else if (colLower.includes('open_price') || colLower.includes('close_price') || 
             colLower.includes('high_price') || colLower.includes('low_price') ||
             colLower.includes('underlying_open') || colLower.includes('underlying_close') ||
             colLower.includes('underlying_high') || colLower.includes('underlying_low') ||
             colLower.includes('underlying_spot')) {
    return 'price';
  } else if (colLower.includes('yield')) {
    return 'yield';
  } else if (colLower.includes('otm')) {
    return 'otm';
  }
  return null;
}

function parseCSVLine(line) {
  const values = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      values.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  
  values.push(current);
  return values;
}

