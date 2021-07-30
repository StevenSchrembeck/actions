import * as Hub from "../../../hub";

export default class FacebookFormBuilder {

    async generateActionForm(actionRequest: Hub.ActionRequest) {
      console.log("Logging this thing to compile... " + actionRequest)
      let form = new Hub.ActionForm()
      form.fields = [{ // TODO replace
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
  
    async generateLoginForm(actionRequest: Hub.ActionRequest) {
      const payloadString = JSON.stringify({ stateUrl: actionRequest.params.state_url })
      
      //  Payload is encrypted to keep things private and prevent tampering
      let encryptedPayload
      try {
        const actionCrypto = new Hub.ActionCrypto()
        encryptedPayload = await actionCrypto.encrypt(payloadString)
      } catch (e) {
        console.log("error", "Payload encryption error:", e.toString())
        throw e
      }
  
      // Step 1 in the oauth flow - user clicks the button in the form and visits the AH url generated here.
      // That response will be auto handled by the AH server as a redirect to the result of oauthUrl function below.
      const startAuthUrl =
        `${process.env.ACTION_HUB_BASE_URL}/actions/facebook_customer_match/oauth?state=${encryptedPayload}`
  
      console.log("debug", "login form has startAuthUrl=", startAuthUrl)
  
      const form = new Hub.ActionForm()
      form.state = new Hub.ActionState()
      form.state.data = "reset"
      form.fields = []
      form.fields.push({
        name: "login",
        type: "oauth_link",
        label: "Log in to Facebook",
        description: "In order to use Facebook Customer Match as a destination, you will need to log in" +
          " once to your Facebook account.",
        oauth_url: startAuthUrl,
      })
      return form
    }
  }