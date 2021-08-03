import * as gaxios from "gaxios"
import * as Hub from "../../hub"
import {URL} from "url"
import * as querystring from "querystring"
import FacebookCustomerMatchExecutor from "./lib/executor"
import FacebookFormBuilder from "./lib/form_builder"
import {sanitizeError} from "./lib/util";
import FacebookCustomerMatchApi from "./lib/api"
// const LOG_PREFIX = "[FB Ads Customer Match]"

export class FacebookCustomerMatchAction extends Hub.OAuthAction {

  readonly name = "facebook_customer_match"
  readonly label = "Facebook Customer Match"
  readonly iconName = "facebookcustomermatch/facebook_ads_icon.png"
  readonly description = "TODO."
  readonly supportedActionTypes = [Hub.ActionType.Query]
  readonly supportedFormats = [Hub.ActionFormat.JsonLabel]
  readonly supportedFormattings = [Hub.ActionFormatting.Unformatted]
  readonly supportedVisualizationFormattings = [Hub.ActionVisualizationFormatting.Noapply]
  readonly supportedDownloadSettings = [Hub.ActionDownloadSettings.Url]
  readonly usesStreaming = true
  readonly requiredFields = []
  readonly params = []

  readonly oauthClientId: string
  readonly oauthClientSecret: string

  constructor(oauthClientId: string, oauthClientSecret: string) {
    super()
    this.oauthClientId = oauthClientId
    this.oauthClientSecret = oauthClientSecret
  }

  async execute(hubRequest: Hub.ActionRequest) {
    let response = new Hub.ActionResponse()
    const accessToken = this.getAccessTokenFromRequest(hubRequest)
    if(!accessToken) {
      response.state = new Hub.ActionState()
      response.state.data = "reset"
      response.success = false
      response.message = "Failed to execute Facebook Customer Match due to missing authentication credentials. No data sent to Facebook. Please try again or contact support"
      return response
    }
    const executor = new FacebookCustomerMatchExecutor(hubRequest, true, accessToken)
    await executor.run()
    return response;
  }

  async form(hubRequest: Hub.ActionRequest) {
    const formBuilder = new FacebookFormBuilder();
    try {
      const isAlreadyAuthenticated = await this.oauthCheck(hubRequest) 
      const accessToken = this.getAccessTokenFromRequest(hubRequest)
      if(isAlreadyAuthenticated && accessToken){
        const facebookApi = new FacebookCustomerMatchApi(accessToken)
        const actionForm = formBuilder.generateActionForm(hubRequest, facebookApi)
        return actionForm
      }
    } catch (err) {
      sanitizeError(err);
      console.error(err);
    }

    // Return the login form to start over if anything goes wrong during authentication or form construction
    // If a user is unauthenticated they are expected to hit an error above
    const loginForm = formBuilder.generateLoginForm(hubRequest);
    return loginForm;
  }

  async oauthUrl(redirectUri: string, encryptedState: string) {
    const url = new URL("https://www.facebook.com/v11.0/dialog/oauth")
    url.search = querystring.stringify({
      client_id: process.env.FACEBOOK_CLIENT_ID,
      redirect_uri: redirectUri,
      state: encryptedState,
    })    
    return url.toString()
  }

  async oauthFetchInfo(urlParams: { [key: string]: string }, redirectUri: string) {
    // urlparams: {code: 'AQBKv0AuStqhveCt8wUpZFdbSrJ9PjhqRxFs-_DXeqaQrB…OaIe2txeIelt5KsEvPSpFPJuzWxdCiZZp9AIR10Qk4cJQ', 
    // state: '1043022mBJGxURyH_G_FhVZO4KlMgCT2-RAjBN_WHZ0Ze…_dlcjtHfS7wk4lA__Mx-cOUl9-jjUdGPGeNDaLDA6kkug'
    // }
    // redirecturi: 'https://looker-action-hub-fork.herokuapp.com/actions/facebook_customer_match/oauth_redirect'
    // plaintext becomes: '{"stateUrl":"https://4mile.looker.com/action_hub_state/NjXxs7CpFyh9NhGxtbrXJv5bDVMCPDsFSD4ZgqQN"}'
    // payload becomes: {stateUrl: 'https://4mile.looker.com/action_hub_state/NjXxs7CpFyh9NhGxtbrXJv5bDVMCPDsFSD4ZgqQN'}

    let plaintext
    try {
      const actionCrypto = new Hub.ActionCrypto()
      plaintext = await actionCrypto.decrypt(urlParams.state)
    } catch (err) {
      console.log("error", "Encryption not correctly configured: ", err.toString())
      throw err
    }

    const payload = JSON.parse(plaintext)
    
    // adding our app secret to the mix gives us a long-lived token (which lives ~60 days) instead of short-lived token
    const longLivedTokenRequestUri = `https://graph.facebook.com/v11.0/oauth/access_token?client_id=${this.oauthClientId}&redirect_uri=${redirectUri}&client_secret=${this.oauthClientSecret}&code=${urlParams.code}`;
    const longLivedTokenResponse = await gaxios.request<any>({method: 'GET', url: longLivedTokenRequestUri})
    
    const longLivedToken = longLivedTokenResponse.data.access_token;
    const tokens = {longLivedToken}
    const userState = { tokens, redirect: redirectUri }

    // So now we use that state url to persist the oauth tokens
    try {
      await gaxios.request({
        method: "POST",
        url: payload.stateUrl,
        data: userState,
      })
    } catch (err) {
      sanitizeError(err)
      // We have seen weird behavior where Looker correctly updates the state, but returns a nonsense status code
      if (err instanceof gaxios.GaxiosError && err.response !== undefined && err.response.status < 100) {
        console.log("debug", "Ignoring state update response with response code <100")
      } else {
        console.log("error", "Error sending user state to Looker:", err && err.toString())
        throw err
      }
    }
  }


  /*
    Facebook expired responses look like (in v11):
    {
      "error": {
        "message": "Error validating access token: Session has expired on Thursday, 29-Jul-21 10:00:00 PDT. The current time is Friday, 30-Jul-21 06:41:07 PDT.",
        "type": "OAuthException",
        "code": 190,
        "error_subcode": 463,
        "fbtrace_id": "A_muLgNXB2rhzyBV_3YbJeo"
      }
    }
  */
  async oauthCheck(request: Hub.ActionRequest): Promise<boolean> {
    try {
      const accessToken = this.getAccessTokenFromRequest(request)
      if (!accessToken) {
        console.log("Failed oauthCheck because access token was missing or malformed")
        return false
      }
      const userDataRequestUri = `https://graph.facebook.com/v11.0/me?access_token=${accessToken}`;
      const userDataResponse = await gaxios.request<any>({method: 'GET', url: userDataRequestUri})      
      if (userDataResponse.data.error && userDataResponse.data.error.message) {
        console.log("Failed oauthCheck because access token was expired or due to an error: " + userDataResponse.data.error.message)
        return false;
      }
      console.log("OAUTH CHECK PASSED")
      return true
    } catch (err) {
      sanitizeError(err)
      console.log("Failed oauthCheck because access token was expired or due to an error: " + err)
      return false;
    }
  }

  protected getAccessTokenFromRequest(request: Hub.ActionRequest) : string | null {
    try {
      const params: any = request.params;
      return JSON.parse(params.state_json).tokens.longLivedToken;
    } catch (err) {
      console.error("Failed to parse state for access token.")
      return null;
    }
  }
}







/******** Register with Hub if prereqs are satisfied ********/

if (process.env.FACEBOOK_CLIENT_ID
  && process.env.FACEBOOK_CLIENT_SECRET
  ) {
    const fcma = new FacebookCustomerMatchAction(
      process.env.FACEBOOK_CLIENT_ID,
      process.env.FACEBOOK_CLIENT_SECRET
    );
    Hub.addAction(fcma);
}
