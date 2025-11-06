import React, { useState, useEffect } from 'react';
import './App.css';

export default function App() {
  const [selectedSources, setSelectedSources] = useState([]);
  const [documents, setDocuments] = useState([]);
  const [savedDocuments, setSavedDocuments] = useState([]);
  const [loading, setLoading] = useState(false);
  const [loadingDB, setLoadingDB] = useState(false);
  const [error, setError] = useState('');
  const [scraped, setScraped] = useState(false);
  const [showSaved, setShowSaved] = useState(false);
  const [totalInDB, setTotalInDB] = useState(0);

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

  const handleScrape = async () => {
    if (selectedSources.length === 0) {
      setError('Please select at least one source');
      return;
    }

    setLoading(true);
    setError('');
    setDocuments([]);
    setScraped(false);
    setShowSaved(false);

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
      setDocuments(data.documents);
      setScraped(true);
      
      // Refresh saved documents count
      fetchSavedDocuments();
    } catch (err) {
      setError('Failed to fetch documents. Please ensure the backend and MongoDB are running.');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const fetchSavedDocuments = async () => {
    setLoadingDB(true);
    try {
      const response = await fetch('http://localhost:8000/documents?limit=100');
      if (response.ok) {
        const data = await response.json();
        setSavedDocuments(data.documents);
        setTotalInDB(data.total);
      }
    } catch (err) {
      console.error('Error fetching saved documents:', err);
    } finally {
      setLoadingDB(false);
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
        setSavedDocuments([]);
        setTotalInDB(0);
        alert('Database cleared successfully');
      }
    } catch (err) {
      alert('Failed to clear database');
      console.error(err);
    }
  };

  useEffect(() => {
    fetchSavedDocuments();
  }, []);

  return (
    <div className="container">
      <div className="content">
        {/* Header */}
        <div className="header">
          
          
        </div>

        {/* Database Info */}
        <div className="db-info">
          <div className="db-stat">
            <span className="db-label">Documents in Database:</span>
            <span className="db-value">{totalInDB}</span>
          </div>
          <div className="db-actions">
            <button onClick={fetchSavedDocuments} className="btn-secondary" disabled={loadingDB}>
              {loadingDB ? 'Loading...' : 'Refresh'}
            </button>
            <button onClick={() => setShowSaved(!showSaved)} className="btn-secondary">
              {showSaved ? 'Hide Saved' : 'View Saved'}
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

          {/* Scrape Button */}
          <div className="section">
            <button
              onClick={handleScrape}
              disabled={loading}
              className={`scrape-button-full ${loading ? 'scrape-button-disabled' : ''}`}
            >
              {loading ? 'Scraping and Saving to Database...' : 'Scrape & Save to MongoDB'}
            </button>
          </div>

          {/* Error Message */}
          {error && (
            <div className="error-box">
              <span className="error-icon">⚠</span>
              <p className="error-text">{error}</p>
            </div>
          )}
        </div>

        {/* Show Saved Documents */}
        {showSaved && (
          <div className="results-panel">
            <div className="results-header">
              <h2 className="results-title">Saved Documents (Database)</h2>
              <span className="results-count">{savedDocuments.length} documents</span>
            </div>

            {savedDocuments.length === 0 ? (
              <div className="no-results">
                <p>No documents in database yet</p>
              </div>
            ) : (
              <div className="articles-grid">
                {savedDocuments.map((doc, idx) => (
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
            )}
          </div>
        )}

        {/* Results from Latest Scrape */}
        {scraped && !showSaved && (
          <div className="results-panel">
            <div className="results-header">
              <h2 className="results-title">Latest Scrape Results</h2>
              <span className="results-count">{documents.length} documents found & saved</span>
            </div>

            {documents.length === 0 ? (
              <div className="no-results">
                <p>No documents found</p>
              </div>
            ) : (
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
            )}
          </div>
        )}
      </div>
    </div>
  );
}