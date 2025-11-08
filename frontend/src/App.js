import React, { useState, useEffect } from 'react';
import './App.css';

export default function App() {
  const [selectedSources, setSelectedSources] = useState([]);
  const [documents, setDocuments] = useState([]);
  const [loading, setLoading] = useState(false);
  const [loadingDisplay, setLoadingDisplay] = useState(false);
  const [error, setError] = useState('');
  const [successMessage, setSuccessMessage] = useState('');
  const [totalInDB, setTotalInDB] = useState(0);
  const [keywordInput, setKeywordInput] = useState('');
  const [keywordEntries, setKeywordEntries] = useState([]);

  const sources = [
    { id: 'RBI', name: 'Reserve Bank of India' },
    { id: 'Income Tax', name: 'Income Tax India' },
    { id: 'GST Council', name: 'GST Council' }
  ];

  const handleSourceToggle = (sourceId) => {
    setSelectedSources(prev => 
      prev.includes(sourceId) 
        ? prev.filter(s => s !== sourceId)
        : [...prev, sourceId]
    );
  };

  const addKeywordEntry = () => {
    if (keywordInput.trim()) {
      setKeywordEntries(prev => [...prev, keywordInput.trim()]);
      setKeywordInput('');
    }
  };

  const removeKeywordEntry = (index) => {
    setKeywordEntries(prev => prev.filter((_, i) => i !== index));
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      addKeywordEntry();
    }
  };

  // Function to scrape and save to DB only (no display)
  const handleScrapeAndSave = async () => {
    if (selectedSources.length === 0) {
      setError('Please select at least one source');
      return;
    }

    setLoading(true);
    setError('');
    setSuccessMessage('');

    try {
      const response = await fetch('http://localhost:8000/scrape', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          sources: selectedSources
        })
      });

      if (!response.ok) {
        throw new Error('Failed to scrape documents');
      }

      const data = await response.json();
      setSuccessMessage(`Successfully scraped and saved ${data.total} documents to database!`);
      
      // Update the total count
      fetchTotalCount();
    } catch (err) {
      setError('Failed to scrape and save documents. Please ensure the backend and MongoDB are running.');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  // Function to display documents from DB with filtering
  const handleDisplayFromDB = async () => {
    if (selectedSources.length === 0) {
      setError('Please select at least one source to display documents');
      return;
    }

    setLoadingDisplay(true);
    setError('');
    setSuccessMessage('');

    try {
      // Use keyword entries directly (each entry is treated as a phrase)
      const keywords = keywordEntries.length > 0 ? keywordEntries : null;

      // Use the filter endpoint with keywords
      const response = await fetch('http://localhost:8000/documents/filter?limit=1000', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          sources: selectedSources,
          keywords: keywords
        })
      });

      if (!response.ok) {
        throw new Error('Failed to fetch documents from database');
      }

      const data = await response.json();
      setDocuments(data.documents);
      
      if (data.documents.length === 0) {
        const keywordText = keywords ? ` with keywords: ${keywords.join(', ')}` : '';
        setError(`No documents found for selected sources: ${selectedSources.join(', ')}${keywordText}`);
      } else {
        const keywordText = keywords ? ` matching ANY of these keywords: ${keywords.join(', ')}` : '';
        setSuccessMessage(`Displaying ${data.documents.length} documents from ${selectedSources.join(', ')}${keywordText}`);
      }
    } catch (err) {
      setError('Failed to fetch documents from database. Please ensure MongoDB is running.');
      console.error(err);
    } finally {
      setLoadingDisplay(false);
    }
  };

  // Fetch only the total count
  const fetchTotalCount = async () => {
    try {
      const response = await fetch('http://localhost:8000/documents?limit=1');
      if (response.ok) {
        const data = await response.json();
        setTotalInDB(data.total);
      }
    } catch (err) {
      console.error('Error fetching total count:', err);
    }
  };

  const clearDatabase = async () => {
    if (!window.confirm('Are you sure you want to clear all documents from the database?')) {
      return;
    }

    try {
      const response = await fetch('http://localhost:8000/documents', {
        method: 'DELETE'
      });
      if (response.ok) {
        setDocuments([]);
        setTotalInDB(0);
        setSuccessMessage('Database cleared successfully');
      }
    } catch (err) {
      setError('Failed to clear database');
      console.error(err);
    }
  };

  useEffect(() => {
    fetchTotalCount();
  }, []);

  return (
    <div className="container">
      <div className="content">
        {/* Header */}
        <div className="header">
          <h1>Government News Scraper</h1>
        </div>

        {/* Database Info */}
        <div className="db-info">
          <div className="db-stat">
            <span className="db-label">Documents in Database:</span>
            <span className="db-value">{totalInDB}</span>
          </div>
          <div className="db-actions">
            <button onClick={fetchTotalCount} className="btn-secondary">
              Refresh Count
            </button>
            <button onClick={clearDatabase} className="btn-danger">
              Clear DB
            </button>
          </div>
        </div>

        {/* Controls */}
        <div className="control-panel">
          {/* Source Selection */}
          <div className="section">
            <label className="label">Select Sources</label>
            <div className="source-grid">
              {sources.map(source => (
                <button
                  key={source.id}
                  onClick={() => handleSourceToggle(source.id)}
                  className={`source-button ${selectedSources.includes(source.id) ? 'source-button-active' : ''}`}
                >
                  {source.name}
                </button>
              ))}
            </div>
          </div>

          {/* Keyword Input */}
          <div className="section" style={{ marginTop: '1.5rem' }}>
            <label className="label">Filter by Keywords (Optional)</label>
            <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.75rem' }}>
              <input
                type="text"
                value={keywordInput}
                onChange={(e) => setKeywordInput(e.target.value)}
                onKeyPress={handleKeyPress}
                placeholder='Enter keyword/phrase (e.g., "corporate finance") and press Enter or Add'
                style={{
                  flex: 1,
                  padding: '12px',
                  border: '1px solid #d1d5db',
                  borderRadius: '6px',
                  fontSize: '14px',
                  fontFamily: 'inherit'
                }}
              />
              <button
                onClick={addKeywordEntry}
                style={{
                  padding: '12px 20px',
                  backgroundColor: '#3b82f6',
                  color: 'white',
                  border: 'none',
                  borderRadius: '6px',
                  cursor: 'pointer',
                  fontWeight: '600',
                  fontSize: '14px'
                }}
              >
                Add
              </button>
            </div>
            
            {/* Display keyword entries as tags */}
            {keywordEntries.length > 0 && (
              <div style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: '0.5rem',
                marginBottom: '0.75rem'
              }}>
                {keywordEntries.map((keyword, index) => (
                  <div
                    key={index}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem',
                      backgroundColor: '#dbeafe',
                      border: '1px solid #3b82f6',
                      borderRadius: '6px',
                      padding: '6px 12px',
                      fontSize: '14px'
                    }}
                  >
                    <span style={{ color: '#1e40af' }}>{keyword}</span>
                    <button
                      onClick={() => removeKeywordEntry(index)}
                      style={{
                        background: 'none',
                        border: 'none',
                        color: '#ef4444',
                        cursor: 'pointer',
                        fontWeight: 'bold',
                        fontSize: '16px',
                        padding: 0,
                        lineHeight: 1
                      }}
                    >
                      ×
                    </button>
                  </div>
                ))}
              </div>
            )}
            
            <p style={{
              fontSize: '13px',
              color: '#6b7280',
              marginTop: '0.5rem',
              marginBottom: 0
            }}>
              • Each entry is treated as an exact phrase (e.g., "corporate finance" matches the whole phrase)<br/>
              • Add multiple keyword phrases - documents matching ANY phrase will be displayed<br/>
              • Leave empty to display all documents from selected sources
            </p>
          </div>

          {/* Two Separate Buttons */}
          <div style={{ marginTop: "1.5rem" }}>
            <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>

              <button
                onClick={handleScrapeAndSave}
                disabled={loading}
                style={{
                  flex: 1,
                  minWidth: '200px',
                  padding: "12px 18px",
                  backgroundColor: loading ? "#9ca3af" : "#2563eb",
                  border: "none",
                  borderRadius: "6px",
                  cursor: loading ? "not-allowed" : "pointer",
                  color: "white",
                  fontWeight: "600",
                  fontSize: "15px",
                  transition: "0.3s",
                }}
              >
                {loading ? 'Scraping & Saving...' : 'Scrape & Save to MongoDB'}
              </button>

              <button
                onClick={handleDisplayFromDB}
                disabled={loadingDisplay}
                style={{
                  flex: 1,
                  minWidth: '200px',
                  padding: "12px 18px",
                  backgroundColor: loadingDisplay ? "#9ca3af" : "#10b981",
                  border: "none",
                  borderRadius: "6px",
                  cursor: loadingDisplay ? "not-allowed" : "pointer",
                  color: "white",
                  fontWeight: "600",
                  fontSize: "15px",
                  transition: "0.3s",
                }}
              >
                {loadingDisplay ? 'Loading...' : 'Display Documents from DB'}
              </button>

            </div>
          </div>

          {/* Success Message */}
          {successMessage && (
            <div className="success-box" style={{
              backgroundColor: '#d1fae5',
              border: '1px solid #10b981',
              borderRadius: '8px',
              padding: '1rem',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem'
            }}>
              <span style={{ fontSize: '1.5rem' }}>✓</span>
              <p style={{ margin: 0, color: '#065f46' }}>{successMessage}</p>
            </div>
          )}

          {/* Error Message */}
          {error && (
            <div className="error-box">
              <span className="error-icon">⚠</span>
              <p className="error-text">{error}</p>
            </div>
          )}
        </div>

        {/* Display Documents */}
        {documents.length > 0 && (
          <div className="results-panel">
            <div className="results-header">
              <h2 className="results-title">Documents from Database</h2>
              <span className="results-count">{documents.length} documents</span>
            </div>

            <div className="articles-grid">
              {documents.map((doc, idx) => (
                <div key={idx} className="article-card">
                  <div className="source-badge">
                    {doc.website}
                  </div>
                  
                  <h3 className="article-title">
                    {doc.title}
                  </h3>
                  
                  {doc.description && (
                    <p className="article-description">
                      {doc.description.length > 150 
                        ? doc.description.substring(0, 150) + '...'
                        : doc.description
                      }
                    </p>
                  )}

                  {doc.date && (
                    <p className="article-date">
                      Date: {doc.date}
                    </p>
                  )}

                  <div className="article-content-preview">
                    {doc.content.substring(0, 200)}...
                  </div>
                  
                  <a
                    href={doc.link}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="article-link"
                  >
                    View Document →
                  </a>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
