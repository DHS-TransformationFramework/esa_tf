const OIDC_ACTIVE = Boolean(process.env.OIDC_ACTIVE);
const OIDC_ROOT_URL = process.env.OIDC_ROOT_URL;
const REALM_NAME = process.env.REALM_NAME;
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const OIDC_ENDPOINT = `${OIDC_ROOT_URL}/auth/realms/${REALM_NAME}/protocol/openid-connect/token/introspect`;

const KEYCLOAK_HOST_HEADER = process.env.KEYCLOAK_HOST_HEADER;
const GUARD_ROLE = process.env.GUARD_ROLE || null;

function getFormBody(token) {
  const form = {
    client_secret: CLIENT_SECRET,
    client_id: CLIENT_ID,
    token: token,
  };
  const formBody = [];
  for (let property in form) {
    const encodedKey = encodeURIComponent(property);
    const encodedValue = encodeURIComponent(form[property]);
    formBody.push(encodedKey + "=" + encodedValue);
  }
  return formBody;
}

async function authorize(r) {
  if (
    OIDC_ACTIVE &&
    // When OIDC is active, following information are required
    (!OIDC_ROOT_URL || !REALM_NAME || !CLIENT_ID || !CLIENT_SECRET)
  ) {
    // Check if we have everything for OIDC connection
    const message = `Cannot properly configure OIDC connection. Check required environment variables.`;
    r.error(message);
    r.return(500, message);
    return;
  }

  r.headersOut["X-Forwarded-For"] = r.headersIn["Host"];
  let token = r.headersIn.Authorization;
  if (!token) {
    r.return(401, "No Authorization header found");
    return;
  }
  token = token.replace("Bearer ", "");

  try {
    const formBody = getFormBody(token);

    const headers = {
      "Content-Type": "application/x-www-form-urlencoded",
    };
    if (KEYCLOAK_HOST_HEADER) {
      headers.Host = KEYCLOAK_HOST_HEADER;
    }

    const response = await ngx.fetch(OIDC_ENDPOINT, {
      method: "POST",
      headers: headers,
      body: formBody.join("&"),
    });
    const json = await response.json();
    const data = {
      status: response.status,
      json,
    };

    r.log(JSON.stringify(json, undefined, 2));

    const status = data.status;
    if (status >= 400 && status < 500) {
      r.warn(`Cannot authenticate on ${OIDC_ENDPOINT} (status ${status})`);
      r.return(status);
      return;
    }

    if (!json.active) {
      r.warn(`Session not found: ${status}`);
      r.return(401, "Session not found");
      return;
    }

    const roles = Object.entries(json.resource_access).reduce(
      (roles, current) => {
        const currentClient = current[0];
        const currentRoles = current[1];
        currentRoles.roles.forEach((r) => {
          roles.push(`${currentClient}:${r}`);
        });
        return roles;
      },
      []
    );
    r.log(`Roles: ${roles.join(", ")}`);
    if (GUARD_ROLE && !roles.includes(GUARD_ROLE)) {
      r.return(
        403,
        `User must have role '${GUARD_ROLE}' to access this resource.`
      );
      return;
    }

    r.headersOut["X-Username"] = json.sub;
    r.headersOut["X-Roles"] = roles.join(",");

    r.status = data.status || 200;
    return true;
  } catch (err) {
    r.warn(`Cannot authenticate: ${err.message}`);
    r.return(err.status || 500);
  }
}

async function authorize_odpapi(r) {
  let success = true;
  if (OIDC_ACTIVE) {
    success = await authorize(r);
  }
  if (success) {
    r.internalRedirect("@odpapi-backend");
  }
}

async function authorize_download(r) {
  let success = true;
  if (OIDC_ACTIVE) {
    success = await authorize(r);
  }
  if (success) {
    r.internalRedirect("@download-backend");
  }
}

export default { authorize_odpapi, authorize_download };
