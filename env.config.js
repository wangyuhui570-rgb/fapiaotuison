try {
  window["STATIC_ENV_CONFIG"] = {
    "ROUTER_PREFIX": "",
    "API_PREFIX": "",
    "RESOURCE_PREFIX": "",
    "VUE_APP_ENV": "prod",
    "VUE_APP_MODEL": "online",
    "VUE_APP_API_TIMEOUT": "15000"
  };
  document["STATIC_ENV_CONFIG"] = window["STATIC_ENV_CONFIG"];
} catch(error) {
  console.error(error.message);
}
  