import https from "https";
import jose from "node-jose";
import { config } from "dotenv"

config()

const { AWS_COGNITO_POOL_ID, AWS_COGNITO_REGION, AWS_COGNITO_APP_CLIENT_ID } =
  process.env;

export async function buildVerifier() {
    const verifier = await new Promise((resolve, reject) => {
        return https.get(keysUrl, (response) => {
            if (response.statusCode === 200) {
                response.on("data", (body) => {
                    const { keys } = JSON.parse(body);
                    jose.JWK.asKeyStore(keys)
                        .then((keystore) => {
                            return resolve(jose.JWS.createVerify(keystore))
                        })
                        .catch(() => {
                            return resolve(null)
                        });
                    });
            } else {
                return resolve(null);
            }
        });
    })
    return verifier;
}

async function authorization(req, res, verifier) {
    const tokenHeader = req.headers.authorization;
    if (!tokenHeader) {
        res.statusCode = 403;
        res.errorMessage = "Forbidden: no token provided";
          return false;
    }
    const parts = tokenHeader.split(" ");
    if (parts.length !== 2) {
        res.statusCode = 403;
        res.errorMessage = "Forbidden: no token provided";
          return false;
    }
      const scheme = parts[0];
      const token = parts[1];
      if (!verifier) {
        res.statusCode = 403;
        res.errorMessage = "Forbidden: could not load public keys";
          return false;
      }

      if (/^Bearer$/i.test(scheme)) {
        return verifier
          .verify(token)
          .then((result) => {
            const claims = JSON.parse(result.payload);
            const currentTS = Math.floor(new Date() / 1000);
            if (currentTS > claims.exp) {
                res.statusCode = 401
                res.errorMessage = "Token is expired"
                return false;
            }
            if (claims.aud !== AWS_COGNITO_APP_CLIENT_ID) {
                res.statusCode = 401 
                res.errorMessage = "Token was not issued for this audience"
              return false ;
            }
            return true
          })
          .catch(() => {
            res.statusCode = 401
            res.errorMessage = "Signature verification failed";
        return false;
        });
      }
      res.statuscode = 403
      res.errorMessage = "Token is not of the right format"
      return false;
}
export default authorization