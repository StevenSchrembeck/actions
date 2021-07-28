import * as Hub from "../../hub";

// const LOG_PREFIX = "[FB Ads Customer Match]"

export class FacebookCustomerMatchAction extends Hub.Action {

  readonly name = "facebook_ads_customer_match"
  readonly label = "Facebook Ads Customer Match"
  readonly iconName = "facebook-customer-match/facebook_ads_icon.png"
  readonly description = "TODO."
  readonly supportedActionTypes = [Hub.ActionType.Query]
  readonly supportedFormats = [Hub.ActionFormat.JsonLabel]
  readonly supportedFormattings = [Hub.ActionFormatting.Unformatted]
  readonly supportedVisualizationFormattings = [Hub.ActionVisualizationFormatting.Noapply]
  readonly supportedDownloadSettings = [Hub.ActionDownloadSettings.Url]
  readonly usesStreaming = true
  readonly requiredFields = []
  readonly params = []


  async execute(_request: Hub.ActionRequest) {
    return new Hub.ActionResponse();
  }

}

Hub.addAction(new FacebookCustomerMatchAction())