# Konzept: Statistiken einfacher verstehen und bedienen

Stand: 16. Oktober 2026. Konzept, noch keine Änderung der Anwendung.

## Ziel

Innerhalb weniger Sekunden drei Fragen beantworten: Wie stehe ich da? Was hat sich verändert? Woher kommt die Veränderung? Details bleiben erreichbar, bestimmen aber nicht den Einstieg.

Grundlage: angemeldete laufende Anwendung auf dem Pi, alle sieben Statistik-Unterpunkte, Monats- und Jahresauswahl, Desktop mit 1440 px und mobile Navigation mit 390 px. Ergänzend wurde die lokale Implementierung untersucht. Lokale Dateien enthalten bereits bestehende Änderungen; sie wurden nicht verändert. Live-Version und lokale Version können voneinander abweichen.

## Befund je geprüftem Schritt

| Schritt | Ansicht / Zustand | Bewertung | Beobachtung und Konsequenz |
|---|---|---|---|
| 1 | Kategorien, Oktober | Verbesserungsbedarf | Ausgaben und Einnahmen haben jeweils Kreisdiagramm plus Aufschlüsselung. Der Verlauf folgt weit unten. Eine klare Zusammenfassung fehlt. Eine sortierte Balkenliste trägt die Hauptaussage kompakter. |
| 2 | Jahresübersicht | Hoher Verbesserungsbedarf | Der übernommene Oktoberfilter reduziert die Jahresübersicht auf eine Zeile plus identische Summenzeile. Der Titel verspricht mehr als der Inhalt. Als Monatsverlauf in den Überblick integrieren. |
| 3 | Vergleich | Hoher Verbesserungsbedarf | Acht Spalten mit unterschiedlichen Bezugsgrößen; laufender Oktober gegen gesamten September. Die wesentliche Veränderung muss aus vielen Zahlen herausgelesen werden. Veränderungen grafisch priorisieren, Vergleichsbasis eindeutig nennen. |
| 4 | Wiederkehrend | Hoher Verbesserungsbedarf | Eine lange Terminliste steht vor den Kennzahlen; danach folgen ein langer Erklärungstext und eine Tabelle mit neun Spalten. Zusammenfassung und wichtigste nächste Termine zuerst, Methoden und Verwaltung auf Nachfrage. |
| 5 | Notwendig/Optional | Teilweise gut | Sinnvolle Trennung und Kennzahlen. Bei Monatsauswahl enthalten die beiden Verlaufsdiagramme nur einen Monat. Die Prozentachse zeigte sehr lange Nachkommastellen. Als Ausgabenperspektive integrieren und Einmonatszustand anders darstellen. |
| 6 | Kumulierte Sparüberweisungen | Teilweise gut | Kennzahlen und Zielbezug sind vorhanden. Ein einzelner Monat eignet sich kaum für einen kumulierten Verlauf. Monatliche und kumulierte Beträge teilen aktuell eine Achse. Kurz „Sparen“ nennen und Darstellungen trennen. |
| 7 | Ausreißer | Verbesserungsbedarf | Gut: Fälle und Erkennungsregel sind sichtbar. Die Spalte „Faktor“ erklärt ihre Bezugsgröße nicht; ein Wert von 1× neben der Regel „mindestens 4-fach“ wirkt widersprüchlich. Auffälligkeiten im Ausgabenkontext erklären. |
| 8 | Kategorien, Jahr 2026 | Verbesserungsbedarf | Viele Kategorien machen die Legende umfangreich. Balkenliste und Kreis konkurrieren weiter um dieselbe Information. Top-Kategorien priorisieren; vollständige Liste zugänglich halten. |
| 9 | Mobile Navigation | Hoher Verbesserungsbedarf | Bei 390 px sind zunächst nur Kategorien, Jahresübersicht und Vergleich sichtbar. Weitere Inhalte liegen in der horizontalen Tab-Leiste außerhalb des sichtbaren Bereichs. Vier kurze, vollständig sichtbare Ziele verwenden. |

Bildschirmbelege bleiben lokal und werden wegen möglicher Finanzdaten nicht versioniert.

Die Vollseitenaufnahmen geben Diagrammflächen teilweise unvollständig wieder. Daraus wird kein dauerhafter Renderfehler der App abgeleitet. Die mobile Aufnahme zeigt das gerenderte Kreisdiagramm. Tastaturbedienung, Screenreader, gemessene Kontraste und alle Lade-/Fehlerzustände sind noch nicht vollständig getestet.

## 1. Vier verständliche Bereiche

Die vier Ziele bleiben auf Desktop und Mobil identisch. Mobil als 2×2-Navigation mit großzügigen berührbaren Flächen; keine horizontal versteckten Ziele und keine zweite Tab-Ebene.

| Bereich | Frage | Bisherige Inhalte |
|---|---|---|
| Überblick | Wie entwickelt sich mein Haushalt? | Zusammenfassung, Jahresübersicht, wichtigste Vergleichsergebnisse |
| Ausgaben | Wofür gebe ich Geld aus und was fällt auf? | Kategorien, Kategorieverlauf, Vergleich, Notwendig/Optional, Ausreißer |
| Sparen | Wie viel lege ich zurück und erreiche ich mein Ziel? | Kumulierte Sparüberweisungen und Sparquote |
| Regelmäßig | Was kommt regelmäßig und was steht als Nächstes an? | Wiederkehrende Ausgaben, Einnahmen, Sparpläne und Termine |

Einnahmen erscheinen im Überblick mit einer aufklappbaren Aufschlüsselung. Sie bleiben gleichwertig in der Zusammenfassung sichtbar, brauchen jedoch kein dauerhaftes zweites Kreisdiagramm.

Der bestehende Cashflow-Dashboard-Einstieg bleibt der schnelle Blick auf den aktuellen Haushalt. Der Statistik-Überblick dient der historischen Analyse und stellt Verlauf, Vergleich und Ursachen nach vorne; er dupliziert nicht sämtliche Dashboard-Kacheln.

## 2. Eine gemeinsame Zeitauswahl

Oberhalb der Bereichsnavigation steht eine gemeinsame, beim Scrollen kompakt sichtbare Zeitsteuerung:

- Auswahl der Zeitart: Monat, Quartal, Jahr oder Zeitraum.
- Daneben die konkrete Auswahl, etwa „Oktober 2026“, mit beschrifteten Zurück-/Weiter-Schaltflächen.
- Ein Klick auf Zurück bedeutet bei Monat September, bei Quartal das vorherige Quartal und bei Jahr das Vorjahr.
- Unter „Zeitraum“: letzte 3 / 6 / 12 Monate, alle Daten oder Von–Bis. Freie Daten werden gemeinsam mit „Anwenden“ übernommen; feste Zeiträume sofort.
- Kein zusätzlicher „Laden“- oder „Vergleichen“-Button nach einer festen Auswahl.
- Tatsächliche Grenzen immer anzeigen, etwa „01.07.–30.09.2026“. Rollierende Fenster müssen ihren Anker nennen, wenn sie am letzten Datenmonat statt an heute enden.
- Zu Beginn die bestehende, gemerkte Auswahl übernehmen. Ohne gespeicherte Auswahl den neuesten Datenmonat zeigen. Liegt dieser zurück, ausdrücklich benennen.
- Bereichswechsel erhält Zeitraum, Vergleich und Filter. Änderungen aus Dashboard/Transaktionen werden wie bisher übernommen; die Statistik macht den aktiven Kontext sichtbar.

Alle Ist-Kennzahlen und Listen verwenden denselben Zeitraum. Ein Kontextdiagramm darf zusätzliche Historie zeigen, muss dann einen eigenen klaren Titel wie „Verlauf der letzten 12 Monate“ tragen und den gewählten Zeitraum markieren. Keine stillschweigende Abweichung.

Ausnahme „Regelmäßig“: aktueller Vertrags-/Serienbestand und zukünftige Termine haben eine andere Zeitlogik. Deshalb dort sichtbar zwischen „Im gewählten Zeitraum gebucht“ und „Aktuell & anstehend“ wählen. Im zweiten Modus wird der historische Zeitraum als nicht anwendbar gekennzeichnet; Termine erhalten eine ausdrücklich separate Auswahl „Nächste 30 / 60 / 90 Tage“. Monatliche Vertragskosten sind normalisierte Werte, keine tatsächlich gebuchte Monatssumme.

## 3. Überblick: zuerst Ergebnis, dann Ursache

Reihenfolge von oben nach unten:

1. Zeitraum und Datenstatus.
2. Vier Kennzahlen: Einnahmen, Ausgaben, Sparüberweisungen und verbleibender Betrag. Jeweils Wert und nachvollziehbare Änderung zur Vergleichsbasis.
3. Breites Verlaufsdiagramm: gruppierte Monatssäulen für Einnahmen und Ausgaben; Sparüberweisungen als klar benannte zusätzliche Serie. Summen und Quoten nicht auf einer gemeinsamen Achse mischen.
4. „Das hat sich verändert“: höchstens drei belegbare Hinweise, mit direktem Weg zur Kategorie oder Buchung.
5. „Monatswerte anzeigen“ öffnet die bisherige Jahresübersicht als Tabelle.

Im Einmonatsmodus zeigt das Hauptdiagramm Wochen oder Tage statt eines nutzlosen einzelnen Monatspunkts. Bei mehreren Monaten Monatswerte verwenden; bei langen Zeiträumen Quartale anbieten und die Aggregation nennen.

Hinweise folgen festen Regeln und nachvollziehbaren Zahlen. Beispiel nur bei passender Datenbasis: „Versicherung: 120 € mehr als im Vergleichszeitraum“. Keine automatische Aussage „gespart“, wenn Daten fehlen oder nur ein Teilmonat vorliegt.

## 4. Ausgaben: eine Liste als Ausgangspunkt

Oben: Gesamtausgaben und Vergleich. Danach eine horizontale, nach Betrag sortierte Balkenliste mit Kategorie, Eurobetrag, Anteil und optionaler Veränderung. Die sechs größten Kategorien zuerst; „Alle Kategorien“ erweitert die Liste. Restbeträge können als „Weitere Kategorien“ zusammengefasst werden, mit Zugang zu den Einzelwerten. Eine bestehende Kategorie „Sonstiges“ darf damit nicht vermischt werden.

Das Kreisdiagramm entfällt als Standard. Eine Balkenliste macht ähnliche Werte, kleine Kategorien und lange Namen leichter vergleichbar und zeigt Zahlen direkt am Objekt.

Ein klar beschrifteter Perspektivwechsel innerhalb derselben Auswertung bietet „Kategorien“, „Notwendig / Optional“ und „Veränderungen“. Mobil als beschriftete Auswahl statt einer weiteren horizontalen Tab-Leiste.

- Kategorien: sortierte Beträge mit Anteilen und Vergleichswerten.
- Notwendig / Optional: bei einem Monat ein geteilter Summenbalken mit beiden Eurobeträgen und Anteilen; bei mehreren Monaten gestapelte Monatssäulen. Optional-Anteil als zusätzliche Ansicht statt ständigem zweiten Großdiagramm. Keine moralische Bewertung optionaler Ausgaben.
- Veränderungen: horizontale Abweichungsbalken um eine Nulllinie, nach absolutem Eurobeitrag sortiert. Direkt „120 € mehr“ oder „80 € weniger“; Prozent ergänzend. Median, gleitender Durchschnitt und Regression unter „Statistische Details“.

Klick auf eine Kategorie öffnet auf Desktop eine Detailfläche rechts, mobil eine vollständige Detailansicht mit Zurück-Funktion. Darin: Betrag, Veränderung, klar datierter Verlauf, zugehörige Buchungen. Kein überraschender Sprung weit nach unten. Ein sichtbarer Filterchip erlaubt das Zurücksetzen; Fokus und Scrollposition bleiben bei der Rückkehr erhalten.

„Auffällige Buchungen“ steht als kompakte Karte innerhalb der Ausgaben. Pro Fall: Händler, Datum, Betrag und verständlicher Grund. Die Vergleichsbasis von Schwelle und Faktor muss eindeutig heißen, etwa „über der Erkennungsschwelle“ oder „gegenüber typischen Einzelbeträgen“.

## 5. Sparen: Monatsleistung und Gesamtverlauf trennen

- „Netto in Sparanlagen überwiesen“ als Hauptwert, darunter Sparüberweisungsquote und Ziel.
- Monatliche Sparbeträge als Säulen.
- Kumulierte Netto-Sparüberweisungen separat als Linie, mit Hinweis „Summe ab Beginn des gewählten Zeitraums“.
- Zielquote als Linie im eigenen Prozentdiagramm oder als kompakte Zielanzeige im Einmonatsmodus.
- Deutlich erklären: Sparüberweisungen bilden weder Depotwert noch Vermögensstand ab. Rücküberweisungen können die Nettosumme reduzieren.
- Ohne Einnahmen keine Quote berechnen; „nicht berechenbar“ statt 0 %. Ohne ausreichende Historie keinen Trend behaupten.

## 6. Regelmäßig: Zusammenfassung vor Terminliste

Oben stehen normalisierte Fixkosten pro Monat, variable wiederkehrende Ausgaben, regelmäßige Einnahmen und Sparpläne. Danach kommen die nächsten drei Termine und „Alle Termine anzeigen“. Die Liste ist chronologisch und gruppiert nach Datum.

Serien werden in „Ausgaben“, „Einnahmen“ und „Sparpläne“ gegliedert. Sichtbar sind Name, tatsächlicher Zahlungsrhythmus, erwarteter Betrag und nächster Termin. Monatlicher Vergleichswert und Jahreswert folgen im Detail. So wird beispielsweise eine jährliche Zahlung nicht als monatliche Abbuchung missverstanden.

„Daten prüfen“, „Inaktiv“ und „Preisänderung“ erhalten klare Statusangaben. Verwaltungsaktionen wie „Nicht mehr wiederkehrend“ wandern in das jeweilige Detail oder Aktionsmenü; die Terminliste wird dadurch ruhiger. Manuelle Einstufungen und ausgeschlossene Serien bleiben zugänglich. Die ausführliche Erkennungslogik öffnet sich über „Wie wird das erkannt?“.

## 7. Vergleich und Datenvertrauen

- Standard für abgeschlossene Monate: vorheriger vollständiger Monat. Für Quartale/Jahre entsprechend Vorquartal/Vorjahr; eine zusätzliche Vorjahresauswahl nur bei vorhandener Basis.
- Laufender Monat: gleicher Kalenderausschnitt des Vormonats, etwa 01.–16.10. gegen 01.–16.09. Beide Grenzen anzeigen. Kalenderkürzungen explizit behandeln.
- Das heutige Datum ist kein Vollständigkeitsnachweis. Fehlt bestätigte Kontenabdeckung, Änderungen neutral als vorläufig anzeigen; keine Erfolgsaussage über vermeintliche Einsparungen.
- Freie Zeiträume mit gleich langem vorherigem Zeitraum vergleichen. Für „Alle Daten“ Vergleich standardmäßig aus, statt stillschweigend auf den letzten Monat auszuweichen.
- Bei Vorwert null keine irreführende Prozentänderung; „neu“ plus Eurodifferenz zeigen. Unbekannte Werte sind keine Nullwerte.
- Ausgaben beruhen auf gebuchtem Nettokonsum einschließlich Erstattungen. Sparen und Umbuchungen getrennt halten.
- Optionaler Ausreißerfilter wirkt konsistent auf alle betroffenen Konsumwerte, Vergleiche, Diagramme und Detailbuchungen. Chip „Ohne auffällige Buchungen“ nennt Anzahl und entfernten Betrag. Er verändert keine zugrunde liegenden Buchungen. Sparen und Einnahmen bleiben nachvollziehbar unberührt.
- Datenstatus neben Zeitraum: Abdeckung vollständig / teilweise / ungeprüft, mit Details pro Konto. Letzte Buchung separat benennen.

## 8. Visuelle und zugängliche Gestaltung

Bestehende Schrift, helle Oberflächen und blaue Bedienakzente weiterverwenden. Größere, aussagekräftige Diagramme und weniger doppelte Karten schaffen die Verbesserung.

- Direkte Zahlenbeschriftungen; Euro sinnvoll gerundet, Prozent maximal eine Nachkommastelle und deutsches Dezimalkomma. Prozentpunkte ausschreiben.
- Stabile Farben für dieselbe Kategorie in allen Ansichten. Einnahmen, Konsum und Sparen mit konsistenter Bedeutung markieren.
- Farbe immer durch Text, Vorzeichen oder Form ergänzen. „Mehr“ oder „weniger“ ist zunächst eine Veränderung, nicht automatisch gut oder schlecht.
- Für Vergleiche eine gemeinsame Skala, bei Balken eine Nullbasis. Keine geglätteten Kurven über Datenlücken.
- Jede Grafik mit kurzer Textzusammenfassung und erreichbarer Datentabelle; Tooltips funktionieren auch bei Fokus und Antippen.
- Bedienflächen möglichst mindestens 44×44 px, sichtbarer Tastaturfokus, programmatische Labels und ausgewählter Navigationszustand. Kontrast gesondert messen.
- Mobil vier Ziele vollständig sichtbar; Diagramme ohne horizontales Seitenscrollen; Buchungen als kompakte Zeilen mit Detailansicht.
- Leerzustände unterscheiden „keine Buchungen“, „nicht importiert“ und „unzureichende Vergleichsdaten“. Während eines Wechsels alten Inhalt nicht unmarkiert unter einem neuen Zeitraum zeigen.

## 9. Umsetzung in sinnvollen Schritten

**Zuerst Bedienung und Konsistenz:** gemeinsame Zeitsteuerung, vier Bereiche, redundant gewordene Ladebuttons entfernen, Zeitraum in allen Abfragen konsistent verwenden, Teilmonatsvergleich und Datenstatus verbessern. Die lokale `loadTimeline()` übergibt aktuell keinen `period`, obwohl der Endpunkt ihn unterstützt; das ist bei der Umsetzung abzugleichen. Ebenso Filtergleichheit zwischen Aggregaten und Detailbuchungen prüfen. Keine neue Backend-Berechnung für bereits vorhandene Informationen erfinden.

**Danach Hauptvisualisierungen:** Überblick mit Monatsverlauf, sortierte Kategorien, Abweichungsbalken, separate Sparverläufe und kompakte Regelmäßig-Zusammenfassung. Tages-/Wochenaggregation und gleich lange Vergleichsfenster benötigen eventuell API-Erweiterungen.

**Zum Schluss Details und Robustheit:** mobile Detailansichten, Statistikdetails, zugängliche Datenansichten, Fehlermeldungen, Tastaturprüfung und visuelle Kontrolle mit vollständigen und lückenhaften Daten.

## 10. Prüfkriterien für die spätere Umsetzung

1. Vorherigen Monat mit einer Aktion erreichen; Jahresauswahl mit höchstens zwei Aktionen.
2. Alle vier Hauptbereiche bei 390 px ohne horizontales Scrollen erreichbar.
3. Zeitraum bleibt bei Bereichswechsel erhalten; sichtbare Ist-Summen und zugehörige Detailbuchungen stimmen überein.
4. Einzelmonat, mehrere Monate, freie Zeiträume und fehlende Daten erzeugen jeweils sinnvolle Darstellungen.
5. Laufender Monat wird nicht kommentarlos mit einem vollständigen Vormonat verglichen.
6. Kategorie mit einer Aktion öffnen; Zurück stellt Kontext und Position wieder her.
7. Vergleichswerte, Netto-Erstattungen, Ausreißerfilter und Sparquoten an gezielten Datenfällen prüfen.
8. Diagramme sind über Text/Tabellen verständlich und zentrale Wege vollständig per Tastatur nutzbar.
9. In kurzen Nutzertests können Personen ohne Erklärung den Zeitraum wechseln, die größte Ausgabenänderung finden und eine Buchung dazu öffnen. Ziel: jeweils innerhalb von 30 Sekunden; dies ist ein Zielwert, kein gemessenes Ergebnis.

## Entscheidungsvorschlag

Vier Bereiche und eine gemeinsame Zeitsteuerung als Grundlage festlegen. Als nächsten Entwurf den Überblick sowie den Weg von einer Kategorie zu ihren Buchungen auf Desktop und Mobil ausarbeiten. Erst danach die Gestaltung auf die übrigen Ansichten übertragen.
