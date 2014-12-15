package isistan.twitter.crawler.util;

import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.PropertyConfiguration;

public class TwitterIDFromName {
	public static void main(String[] args) throws TwitterException {
		System.setProperty("twitter4j.loggerFactory", "twitter4j.internal.logging.NullLoggerFactory");
		String users = "01watch 140Conf 140LoveBird 150c_vapour 19Sixty3 1BizLeader 1M_follow_ME 1_crazy_canuck 1secondfilm 25hourprinting 2MarketingBRITS 2drinksbehind 2klassy 2qoolgal 2travelcuba 30SECONDSTOMARS 3_wise_men 3dReality 493Flash 4ChangingLives 4DeepATL 4alpha 4giv 4sitemarketing 52Limited 5min 74fpv 7_ly 88Vegas88 957BenFM A2DSportsTalk AAFilmFest AAshmam ABC_DWTS AaronG1990 AaronParsons Aaron_Judd AbbottAce AbidW AboutCruises AboutGymnastics AbramoAngel AngelaAshby BirthdayPartyi BlogBrooklyn BrainfitWorld Chandler_Hill Christian411 ChristianBurns D01VYj DJB2328 DansAdventure DavidReis Emmizzz EstevanVanzant GooseRadio Haltzman JasonDoig LovePsychic101 Mini_MiniMarket SFBrawnyBear SaartjieProject Scopus ShootingHoliday SoulySoul UMBizIntel a02toyota a11y aafcric aamardis aaron_ortega abbashaiderali abcflgulf aboslovesyou abpagfh abpaine absolit abster_mill alfredomarwin ashevilleweb bonnyglen buncai chamberbclc chreestie claddah76 cuka_fun dealarchitect dennisroberts dermacontour easymlm figmentations gregbond highheelslofi iamwilliams marcellaartigas markmackinnon martinezdesigns marys_mermaids newscenemag petersng";
		Twitter twitter = new TwitterFactory(new PropertyConfiguration(
				ClassLoader.getSystemResourceAsStream("atomassel1.properties"))).getInstance();
		ResponseList<Status> timeline = twitter.getUserTimeline("BarackObama", new Paging(100, 200));
		for (Status status : timeline) {
			System.out.println(status.getText());
		}
		for (String u : users.split(" ")) {
			try {
				User user = twitter.showUser(u);
				System.out.println("Usuario " + u + " : " + user.getId());
			} catch (Exception e) {
				System.out.println("El usuario no existe: " + u);
				System.out.println(e.getMessage() + " " + e.getCause());
			}
		}

	}
}
