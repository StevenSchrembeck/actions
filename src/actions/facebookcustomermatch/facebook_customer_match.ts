import * as gaxios from "gaxios"
import * as Hub from "../../hub"
import {URL} from "url"
import * as querystring from "querystring"
import FacebookCustomerMatchExecutor from "./lib/executor"
import FacebookFormBuilder from "./lib/form_builder"
import {sanitizeError} from "./lib/util";
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
    console.log("Begin running")
    debugger;
    const executor = new FacebookCustomerMatchExecutor(hubRequest, true);
    await executor.run()
    console.log("Done running!");
    return new Hub.ActionResponse();
  }

  async form(hubRequest: Hub.ActionRequest) {
    try {
      const formBuilder = new FacebookFormBuilder();
      const loginForm = formBuilder.generateLoginForm(hubRequest);
      return loginForm;

    } catch (err) {
      err = sanitizeError(err);
      console.error(err);

      let form = new Hub.ActionForm()
      form.fields = [{
        label: "Test1",
        name: "test1",
        required: true,
        type: "string",
      }, {
        label: "Test2",
        name: "test2",
        required: true,
        type: "string",
      }]
      return form
      }
  }

  async oauthUrl(redirectUri: string, encryptedState: string) {
    const url = new URL("https://www.facebook.com/v11.0/dialog/oauth")
    url.search = querystring.stringify({
      client_id: process.env.FACEBOOK_CLIENT_ID,
      redirect_uri: redirectUri,
      state: encryptedState,
    })
    console.log("Setting fb url as: " + url.toString()) // TODO remove this log.
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

    const customAudienceUrl = `https://graph.facebook.com/v11.0/act_114109700789636/customaudiences?access_token=${longLivedToken}`
    const customAudienceTestResponse = await gaxios.request<any>({method: 'GET', url: customAudienceUrl}).then((stuff) => console.log(JSON.stringify(stuff))).catch((err) => console.log(err))
    console.log('we did it! ' + customAudienceTestResponse)

    const userState = { tokens, redirect: redirectUri }

    // So now we use that state url to persist the oauth tokens
    try {
      await gaxios.request({
        method: "POST",
        url: payload.stateUrl,
        data: userState,
      })
    } catch (err) {
      // We have seen weird behavior where Looker correctly updates the state, but returns a nonsense status code
      if (err instanceof gaxios.GaxiosError && err.response !== undefined && err.response.status < 100) {
        console.log("debug", "Ignoring state update response with response code <100")
      } else {
        console.log("error", "Error sending user state to Looker:", err.toString())
        throw err
      }
    }
  }

  async oauthCheck(request: Hub.ActionRequest) {
    // TODO implement cheeck
    console.log(request)
    return true
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
