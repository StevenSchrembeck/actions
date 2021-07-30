import * as Hub from "../../../hub";

export default class FacebookFormBuilder {

    async generateActionForm(actionRequest: Hub.ActionRequest) {

      console.log("Form params are: " + JSON.stringify(actionRequest.formParams))
      let form = new Hub.ActionForm()
      form.fields = [{ // TODO replace
        label: "Choose a business",
        name: "choose_business",
        required: true,
        interactive: true,
        type: "select" as "select",
        options: [
          {name: "businessA", label: "YOUR DEFAULT BUSINESS HERE"},
          {name: "businessB", label: "Business B"},
          {name: "businessC", label: "Business C"},
        ]
      }]
      if (actionRequest.formParams.choose_business) {
        form.fields.push({
          label: "Choose a Facebook ad account",
          name: "choose_ad_account",
          required: true,
          interactive: true,
          type: "select" as "select",
          options: [
            {name: "adaccount1", label: "Ad account 1"},
            {name: "adaccount2", label: "Ad account 2"},
            {name: "adaccount3", label: "Ad account 3"},
          ]
        })
      }
      if (actionRequest.formParams.choose_ad_account) {
        form.fields.push({
          label: "Would you like to create a new audience, update existing, or replace existing?",
          name: "choose_create_update_replace",
          description: "Replacing deletes all users from the audience then replaces them with new ones",
          required: true,
          interactive: true,
          type: "select" as "select",
          options: [
            {name: "create_audience", label: "Create new audience"},
            {name: "update_audience", label: "Update existing audience"},
            {name: "replace_audience", label: "Replace existing audience"},
          ]
        })
      }
      if (actionRequest.formParams.choose_create_update_replace === "create_audience") {
        form.fields.push({
          label: "New audience name",
          name: "create_audience_name",          
          required: true,
          type: "string",
        })
        form.fields.push({
          label: "New audience description",
          name: "create_audience_description",          
          required: true,
          type: "string",
        })
        form.fields.push({
          label: "Should the data be hashed first?",
          name: "should_hash",
          description: "Yes is appropriate for most users. Only select No if you know your data has already been hashed.",      
          required: true,
          type: "select" as "select",
          options: [
            {name: "do_hashing", label: "Yes"},
            {name: "do_no_hashing", label: "No"},
          ],
          default: "do_hashing"
        })
      } else if (actionRequest.formParams.choose_create_update_replace === "update_audience" ||
                actionRequest.formParams.choose_create_update_replace === "replace_audience") 
      {
        form.fields.push({
          label: "UDPDATE OR REPLACE PLACEHOLDER",
          name: "test",
          type: "string",
        })
      }

      
      return form


      /*
      > serial
      | parallel

      >pick a business id
      >pick an ad account
      >choose create, update, or replace (which is delete all + update)
      if(create) {
        |enter name
        |enter description
        |choose hash or no hash
      } else { // update or replace
        |choose an audience
        |choose hash or no hash 
      }
      */
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