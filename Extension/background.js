chrome.runtime.onInstalled.addListener(() => {
    chrome.contextMenus.create({
      id: "generateResume",
      title: "Generate Resume from Selection",
      contexts: ["selection"]
    });
  });
  
  chrome.contextMenus.onClicked.addListener((info) => {
    chrome.storage.local.set({ selectedText: info.selectionText }, () => {
      chrome.runtime.sendMessage({ action: "generateResume" });
    });
  });