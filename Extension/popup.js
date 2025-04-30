window.handleCredentialResponse = async (response) => {
    const idToken = response.credential;
  
    // Decode JWT token to get user info (optional)
    const base64Url = idToken.split('.')[1];
    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
    const userPayload = JSON.parse(atob(base64));
  
    document.getElementById('status').innerText = `Hello, ${userPayload.name}`;
    
    // Store user info locally or send to Azure Function
    chrome.storage.local.set({ user: userPayload });
  };
  