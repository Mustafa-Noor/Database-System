import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Typography,
  Paper,
  Button,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Alert,
  Divider,
  Grid,
  Tooltip,
  Menu,
  MenuItem,
  CssBaseline,
  Container,
  useTheme
} from '@mui/material';
import {
  Add as AddIcon,
  Share as ShareIcon,
  Delete as DeleteIcon,
  Code as CodeIcon,
  PlayArrow as PlayIcon,
  Lock as LockIcon,
  Key as KeyIcon,
  Storage as StorageIcon,
  TableChart as TableIcon,
  MoreVert as MoreVertIcon,
  Logout as LogoutIcon
} from '@mui/icons-material';
import axios from 'axios';
import { styled } from '@mui/material/styles';

// Styled components
const StyledPaper = styled(Paper)(({ theme }) => ({
  height: '100vh',
  display: 'flex',
  flexDirection: 'column',
  backgroundColor: '#f4f6f9',
  width: '100%',
  overflow: 'hidden'
}));

const PanelHeader = styled(Box)(({ theme }) => ({
  padding: theme.spacing(2),
  backgroundColor: '#ffffff',
  borderBottom: `1px solid ${theme.palette.divider}`,
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(2),
  boxShadow: 'rgba(0, 0, 0, 0.05) 0px 1px 2px 0px',
}));

const QueryEditor = styled(TextField)(({ theme }) => ({
  '& .MuiInputBase-root': {
    backgroundColor: '#ffffff',
    borderRadius: theme.shape.borderRadius,
    boxShadow: 'rgba(0, 0, 0, 0.05) 0px 1px 2px 0px',
    fontFamily: 'Consolas, monospace',
    fontSize: '14px',
  },
  '& .MuiInputBase-input': {
    color: theme.palette.text.primary,
  },
}));

const DatabaseItem = styled(Box)(({ theme }) => ({
  padding: theme.spacing(1.5),
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(1.5),
  cursor: 'pointer',
  borderRadius: theme.shape.borderRadius,
  transition: 'background-color 0.2s',
  '&:hover': {
    backgroundColor: theme.palette.action.hover,
  },
}));

const ResultsPanel = styled(Box)(({ theme }) => ({
  backgroundColor: '#ffffff',
  padding: theme.spacing(2),
  borderRadius: theme.shape.borderRadius,
  boxShadow: 'rgba(0, 0, 0, 0.05) 0px 1px 2px 0px',
  overflow: 'auto',
  fontFamily: 'Consolas, monospace',
  fontSize: '14px',
  maxHeight: '300px',
}));

const Dashboard = ({ user, onLogout }) => {
  const navigate = useNavigate();
  const theme = useTheme();
  const [databases, setDatabases] = useState([]);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [openDialog, setOpenDialog] = useState(false);
  const [dialogType, setDialogType] = useState('');
  const [dialogData, setDialogData] = useState({
    databaseName: '',
    username: '',
    query: '',
  });
  const [queryResults, setQueryResults] = useState(null);
  const [selectedDatabase, setSelectedDatabase] = useState(null);
  const [anchorEl, setAnchorEl] = useState(null);
  const [selectedDbForMenu, setSelectedDbForMenu] = useState(null);
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [resultColumns, setResultColumns] = useState([]);

  useEffect(() => {
    const storedUser = user || JSON.parse(localStorage.getItem('user'));
    if (storedUser) {
      axios.defaults.headers.common['Authorization'] = storedUser.username;
    }
  }, [user]);

  useEffect(() => {
    fetchDatabases();
  }, []);

  const fetchDatabases = async () => {
    try {
      const response = await axios.get('http://localhost:5000/api/databases');
      setDatabases(response.data.databases);
    } catch (err) {
      setError(err.response?.data?.error || err.message || 'Failed to fetch databases');
    }
  };

  const handleCreateDatabase = async () => {
    try {
      await axios.post('http://localhost:5000/api/databases', {
        name: dialogData.databaseName,
      });
      setSuccess('Database created successfully');
      fetchDatabases();
      handleCloseDialog();
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to create database');
    }
  };

  const handleShareDatabase = async () => {
    if (!selectedDatabase) {
      setError('Please select a database first');
      return;
    }

    if (!dialogData.username) {
      setError('Please enter a username to share with');
      return;
    }

    try {
      await axios.post(`http://localhost:5000/api/databases/${selectedDatabase}/share`, {
        username: dialogData.username
      });
      setSuccess('Database shared successfully');
      fetchDatabases();
      handleCloseDialog();
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to share database');
    }
  };

  const handleRevokeAccess = async (databaseName, username) => {
    try {
      await axios.post(`http://localhost:5000/api/databases/${databaseName}/revoke`, {
        username,
      });
      setSuccess('Access revoked successfully');
      fetchDatabases();
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to revoke access');
    }
  };

  const handleExecuteQuery = async () => {
    if (!selectedDatabase) {
      setError('Please select a database first');
      return;
    }

    if (!query.trim()) {
      setError('Please enter a query');
      return;
    }

    setLoading(true);
    setError(null);
    setQueryResults(null);
    setResultColumns(null);

    try {
      const response = await fetch('http://localhost:5000/api/execute_query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': localStorage.getItem('username')
        },
        body: JSON.stringify({
          query: query,
          database: selectedDatabase
        })
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to execute query');
      }

      // Handle different types of results
      if (data.message) {
        // Success message for commands like CREATE TABLE, INSERT, etc.
        setQueryResults([{ message: data.message }]);
        setResultColumns(['Result']);
      } else if (data.results) {
        // Results from SELECT queries
        setQueryResults(data.results);
        setResultColumns(data.columns);
      } else {
        // Generic success message
        setQueryResults([{ message: 'Query executed successfully' }]);
        setResultColumns(['Result']);
      }

    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleOpenDialog = (type, databaseName = '') => {
    setDialogType(type);
    setDialogData({ ...dialogData, databaseName });
    setOpenDialog(true);
  };

  const handleCloseDialog = () => {
    setOpenDialog(false);
    setDialogData({ databaseName: '', username: '', query: '' });
  };

  const handleMenuOpen = (event, db) => {
    setAnchorEl(event.currentTarget);
    setSelectedDbForMenu(db);
  };

  const handleMenuClose = () => {
    setAnchorEl(null);
    setSelectedDbForMenu(null);
  };

  const handleLogout = async () => {
    try {
      const username = localStorage.getItem('username');
      if (!username) {
        onLogout();
        navigate('/login');
        return;
      }

      await axios.post('http://localhost:5000/api/auth/logout', {}, {
        headers: {
          'Authorization': username
        }
      });

      // Clear local storage
      localStorage.removeItem('user');
      localStorage.removeItem('username');
      
      // Call onLogout callback
      onLogout();
      
      // Navigate to login page
      navigate('/login');
    } catch (err) {
      console.error('Logout error:', err);
      // Even if the server request fails, we should still log the user out locally
      localStorage.removeItem('user');
      localStorage.removeItem('username');
      onLogout();
      navigate('/login');
    }
  };

  return (
    <Box sx={{ width: '100vw', height: '100vh', overflow: 'hidden' }}>
      <CssBaseline />
      <StyledPaper>
        <Box sx={{ display: 'flex', height: '100%', width: '100%' }}>
          {/* Left Panel - Database Explorer */}
          <Box 
            sx={{ 
              width: 300, 
              borderRight: `1px solid ${theme.palette.divider}`,
              backgroundColor: '#ffffff',
              height: '100%',
              overflow: 'auto'
            }}
          >
            <PanelHeader>
              <StorageIcon color="primary" />
              <Typography variant="h6" sx={{ fontWeight: 'bold' }}>
                Database Explorer
              </Typography>
              <Box sx={{ flexGrow: 1 }} />
              <Tooltip title="Logout">
                <IconButton onClick={handleLogout} color="error">
                  <LogoutIcon />
                </IconButton>
              </Tooltip>
            </PanelHeader>
            <Box sx={{ p: 2 }}>
              <Button
                fullWidth
                variant="contained"
                startIcon={<AddIcon />}
                onClick={() => handleOpenDialog('create')}
                sx={{ mb: 2 }}
              >
                New Database
              </Button>
              {databases.map((db) => (
                <DatabaseItem
                  key={db.name}
                  onClick={() => setSelectedDatabase(db.name)}
                  sx={{
                    backgroundColor: selectedDatabase === db.name 
                      ? theme.palette.action.selected 
                      : 'transparent',
                  }}
                >
                  {db.type === 'owned' ? <LockIcon color="primary" /> : <KeyIcon color="secondary" />}
                  <Typography sx={{ flex: 1 }}>{db.name}</Typography>
                  <IconButton
                    size="small"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleMenuOpen(e, db);
                    }}
                  >
                    <MoreVertIcon />
                  </IconButton>
                </DatabaseItem>
              ))}
            </Box>
          </Box>

          {/* Right Panel - Query and Results */}
          <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' }}>
            {/* Query Editor */}
            <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
              <PanelHeader>
                <CodeIcon color="primary" />
                <Typography variant="h6" sx={{ fontWeight: 'bold' }}>
                  Query Editor
                </Typography>
                {selectedDatabase && (
                  <Typography variant="body2" color="primary" sx={{ ml: 2 }}>
                    Database: {selectedDatabase}
                  </Typography>
                )}
              </PanelHeader>
              <Box sx={{ p: 2, flex: 1 }}>
                <QueryEditor
                  fullWidth
                  multiline
                  rows={10}
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Enter your SQL query here..."
                  variant="outlined"
                />
                <Box sx={{ mt: 2, display: 'flex', gap: 1 }}>
                  <Button
                    variant="contained"
                    startIcon={<PlayIcon />}
                    onClick={handleExecuteQuery}
                    disabled={!selectedDatabase}
                  >
                    Execute Query
                  </Button>
                  <Button
                    variant="outlined"
                    startIcon={<ShareIcon />}
                    onClick={() => handleOpenDialog('share')}
                    disabled={!selectedDatabase}
                  >
                    Share Database
                  </Button>
                </Box>
              </Box>
            </Box>

            {/* Results Panel */}
            <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
              <PanelHeader>
                <TableIcon color="primary" />
                <Typography variant="h6" sx={{ fontWeight: 'bold' }}>
                  Query Results
                </Typography>
              </PanelHeader>
              <Box sx={{ p: 2, flex: 1 }}>
                {loading && (
                  <Typography color="textSecondary" align="center">
                    Executing query...
                  </Typography>
                )}
                {error && (
                  <Typography color="error" align="center">
                    {error}
                  </Typography>
                )}
                {/* Tabular output for SELECT/JOIN results */}
                {queryResults && Array.isArray(queryResults) && resultColumns && resultColumns.length > 0 && !loading && (
                  <Box sx={{ overflowX: 'auto' }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', marginTop: 16 }}>
                      <thead>
                        <tr>
                          {resultColumns.map((column, idx) => (
                            <th key={idx} style={{ borderBottom: '2px solid #ddd', padding: 8, background: '#f9f9f9', textAlign: 'left' }}>{column}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {queryResults.length === 0 ? (
                          <tr>
                            <td colSpan={resultColumns.length} style={{ textAlign: 'center', padding: 16 }}>
                              No results found.
                            </td>
                          </tr>
                        ) : (
                          queryResults.map((row, rowIdx) => (
                            <tr key={rowIdx} style={{ background: rowIdx % 2 === 0 ? '#fff' : '#f5f5f5' }}>
                              {Array.isArray(row)
                                ? row.map((cell, cellIdx) => (
                                    <td key={cellIdx} style={{ padding: 8, borderBottom: '1px solid #eee' }}>{cell}</td>
                                  ))
                                : resultColumns.map((col, colIdx) => (
                                    <td key={colIdx} style={{ padding: 8, borderBottom: '1px solid #eee' }}>{row[col] || row.message || ''}</td>
                                  ))}
                            </tr>
                          ))
                        )}
                      </tbody>
                    </table>
                  </Box>
                )}
                {/* Fallback for non-tabular/success messages */}
                {queryResults && (!Array.isArray(queryResults) || !resultColumns || resultColumns.length === 0) && !loading && (
                  <Alert severity="success" sx={{ mt: 2 }}>
                    {typeof queryResults === 'string' ? queryResults : queryResults.message || 'Query executed successfully'}
                  </Alert>
                )}
              </Box>
            </Box>
          </Box>
        </Box>

        {/* Status/Error Bar */}
        {(error || success) && (
          <Alert 
            severity={error ? 'error' : 'success'}
            onClose={() => {
              setError('');
              setSuccess('');
            }}
            sx={{ 
              position: 'absolute', 
              bottom: 0, 
              left: 0, 
              right: 0, 
              zIndex: 1000 
            }}
          >
            {error || success}
          </Alert>
        )}

        {/* Database Menu */}
        <Menu
          anchorEl={anchorEl}
          open={Boolean(anchorEl)}
          onClose={handleMenuClose}
        >
          <MenuItem onClick={() => {
            handleOpenDialog('share', selectedDbForMenu?.name);
            handleMenuClose();
          }}>
            <ShareIcon sx={{ mr: 1 }} /> Share Database
          </MenuItem>
          {selectedDbForMenu?.shared_with?.map((username) => (
            <MenuItem
              key={username}
              onClick={() => {
                handleRevokeAccess(selectedDbForMenu.name, username);
                handleMenuClose();
              }}
            >
              <DeleteIcon sx={{ mr: 1, color: 'error.main' }} /> 
              Revoke access from {username}
            </MenuItem>
          ))}
        </Menu>

        {/* Share Dialog */}
        <Dialog open={openDialog} onClose={handleCloseDialog} maxWidth="xs" fullWidth>
          <DialogTitle>
            {dialogType === 'create' && 'Create New Database'}
            {dialogType === 'share' && `Share Database: ${selectedDatabase}`}
          </DialogTitle>
          <DialogContent>
            {dialogType === 'create' && (
              <TextField
                autoFocus
                margin="dense"
                label="Database Name"
                fullWidth
                value={dialogData.databaseName}
                onChange={(e) => setDialogData({ ...dialogData, databaseName: e.target.value })}
                variant="outlined"
              />
            )}
            {dialogType === 'share' && (
              <TextField
                autoFocus
                margin="dense"
                label="Username to Share With"
                fullWidth
                value={dialogData.username}
                onChange={(e) => setDialogData({ ...dialogData, username: e.target.value })}
                variant="outlined"
              />
            )}
          </DialogContent>
          <DialogActions>
            <Button onClick={handleCloseDialog} color="secondary">
              Cancel
            </Button>
            <Button
              onClick={
                dialogType === 'create'
                  ? handleCreateDatabase
                  : handleShareDatabase
              }
              variant="contained"
              color="primary"
            >
              {dialogType === 'create' ? 'Create' : 'Share'}
            </Button>
          </DialogActions>
        </Dialog>
      </StyledPaper>
    </Box>
  );
};

export default Dashboard;