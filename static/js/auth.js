"use strict";

(() => {
  const form = document.getElementById("authForm");
  if (!form) return;
  const mode = document.body.dataset.authPage;
  const status = document.getElementById("authStatus");
  const submit = document.getElementById("authSubmit");
  const username = document.getElementById("username");
  const password = document.getElementById("password");
  const confirm = document.getElementById("confirmPassword");
  const params = new URLSearchParams(window.location.search);

  if (mode === "login" && params.get("registered") === "1") {
    status.textContent = "Account created. Please sign in.";
    status.dataset.kind = "success";
    username.value = params.get("username") || "";
  }

  function show(text, kind = "error") {
    status.textContent = text;
    status.dataset.kind = kind;
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const name = username.value.trim();
    const pwd = password.value;
    if (mode === "register" && confirm.value !== pwd) {
      show("Passwords do not match.");
      return;
    }
    if (mode === "register" && new TextEncoder().encode(pwd).length < 8) {
      show("Password must contain at least 8 UTF-8 bytes.");
      return;
    }
    submit.disabled = true;
    show("Processing…", "info");
    try {
      const response = await fetch(mode === "register" ? "/register" : "/login", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json", "Accept": "application/json" },
        body: JSON.stringify({ username: name, password: pwd })
      });
      let result = {};
      try { result = await response.json(); } catch (_) { /* handled below */ }
      if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
      password.value = "";
      if (mode === "register") {
        window.location.replace(`/login.html?registered=1&username=${encodeURIComponent(name)}`);
      } else {
        window.location.replace("/chat.html");
      }
    } catch (error) {
      show(error.message || "Could not connect to the backend.");
      submit.disabled = false;
    }
  });
})();
