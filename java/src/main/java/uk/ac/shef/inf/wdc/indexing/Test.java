package uk.ac.shef.inf.wdc.indexing;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
            /*
    Poe&#x27;s famous icon, The Raven displayed on 15 oz. coffee mug - also see the Poe mug
    18&quot;x12&quot; Artist print on card stock by Jake Prendez
zzgl. Versandkosten Muß man als Stels \" Pilot \" einfach haben. Stels Tasse mit dem gewissen Aufdruck.....
Pour tous les événements importants de votre vie, pensez à la carte personnalisée. Texte/image à votre convenance au gré de vos envies et de votre fantaisie. Création Carlotta Kapa Voir galerie &quot;boutique créations&quot;. Délai de livraison 7 jours.

     */
        String s="2015 Nissan Pathfinder 2WD 4dr S *Ltd Avail*\n";
        s= StringEscapeUtils.unescapeHtml4(s);
        LanguageDetector detector = LanguageDetector.getDefaultLanguageDetector().loadModels();
        detector.addText(s);

        //longText.append(texts.get(i)).append(" ");
        LanguageResult languageResult = detector.detect();
        //check result
        //return bestLanguage.getLang().equalsIgnoreCase("eng");
        String lang= languageResult.getLanguage();
        float conf = languageResult.getRawScore();
        System.out.println(lang);
    }
}
