# Statistikoberfläche – 16.10.2026

Das [Konzept](statistik-konzept/konzept.md) ist in der bestehenden FastAPI-Anwendung umgesetzt. Die sieben Statistik-Tabs wurden durch Überblick, Ausgaben, Sparen und Regelmäßig ersetzt. Die Änderungen werden mit dem unten dokumentierten Ablauf auf den Pi ausgerollt.

## Bedienung

- Gemeinsame Zeitsteuerung für Monat, Quartal, Jahr, rollierende und freie Zeiträume. Monats-/Quartals-/Jahreswechsel über Zurück/Weiter; feste Auswahlen aktualisieren sofort.
- Freie Zeiträume bleiben beim Wechsel zu Dashboard, Buchungen und Budget erhalten, auch nach erneutem Öffnen der Anwendung.
- Vier vollständig sichtbare Bereiche auf Mobilgeräten. Die Zeitsteuerung bleibt beim Scrollen sichtbar und wird kompakter.
- Kategorien öffnen einen nativen Dialog: seitlich auf Desktop, bildschirmfüllend auf Mobilgeräten. Klick auf die Abdunklung, Escape oder das Schließen-Symbol schließen ihn; der Fokus kehrt zur Kategorie zurück.
- Vergleich, Ausgabenperspektive und Filter bleiben bei Bereichswechsel erhalten. Veraltete Antworten schneller Zeitraumwechsel werden verworfen; Aktualisierung und Fehler werden sichtbar angezeigt.

## Auswertungen

- Überblick: vier Kennzahlen, Tageswerte bei einem einzelnen Monat, Monatswerte bei mehreren Monaten, zusätzlich Quartalsaggregation; größte Euroveränderungen mit direktem Zugang zur Kategorie. Einnahmen und Monatswerte sind aufklappbar.
- Ausgaben: sortierte Balken mit Beträgen und Anteilen, zunächst sechs Kategorien, erweiterbare vollständige Liste. Weitere Perspektiven: Notwendig/Optional und Veränderung gegenüber dem Vorzeitraum. Statistische Details enthalten Median, Durchschnitt der letzten drei beobachteten Monate und Kalendertrend.
- Auffällige Buchungen stehen bei den Ausgaben. Der Faktor ist ausdrücklich auf die Erkennungsschwelle bezogen. Der optionale Filter betrifft Konsumausgaben, einschließlich Kennzahlen, Vergleichen, Verlauf und Kategorie-Buchungen.
- Sparen: monatliche und kumulierte Nettoüberweisungen sowie Sparquote auf getrennten Skalen; ohne positive Einnahmen keine berechenbare Sparquote. Die kumulierte Summe beginnt am gewählten Zeitraum und ist kein Vermögensstand.
- Regelmäßig: Kennzahlen zuerst, die nächsten drei Termine, erweiterbare Liste und Vorschau für 30/60/90 Tage. Ausgaben, Einnahmen und Sparpläne sind getrennt. Normalisierte Monatskosten, Zahlungsrhythmus und Verwaltungsaktionen stehen im Seriendetail. Ein separater Modus zeigt tatsächliche Buchungen zu aktuell aktiven Serien im historischen Zeitraum; beendete historische Serien können dabei fehlen.
- Datenstand pro Konto und Bestätigung der Abdeckung sind einsehbar. Fehlende Gesamtmonate bleiben unbekannt; ausdrücklich bestätigte leere Monate gelten als null. Diagramme bieten zusätzliche Datentabellen.

## Vergleichslogik

Ein laufender Monat wird bis zum heutigen Tag mit demselben Kalenderausschnitt des Vormonats verglichen. Quartale und Jahre verhalten sich entsprechend; Monatsenden und Schaltjahre werden gekürzt. Freie und rollierende Zeiträume verwenden gleich lange vorherige Intervalle. Beide Datumsgrenzen sind sichtbar. Ohne bestätigte Kontenabdeckung sind Veränderungen vorläufig. „Alle Daten“ hat keinen stillschweigenden Vergleich gegen den letzten Monat. Ohne positiven Vorwert wird keine Prozentänderung ausgewiesen.

Die neue Lese-API `/api/stats/explorer` verwendet dieselben Kategoriearten, Buchungsaufteilungen und Ausreißerregeln wie die bestehenden Auswertungen. Sie führt keine Migrationen oder Buchungsänderungen aus. Bestehende API-Endpunkte bleiben verfügbar.

## Prüfung

- `python -m unittest discover -s tests -v`: 83 Tests, 82 bestanden, ein optionaler Test für eine private PDF-Datei übersprungen.
- 14 neue Tests sichern Teilzeiträume, Monatsenden, Schaltjahr, freie Zeiträume, fehlende Daten, bestätigte Nullmonate, Quoten ohne Einnahmen, Aufteilungen, Erstattungen, Vormerkungen, Filtergleichheit und lückenlose Kontenabdeckung ab.
- JavaScript-Syntaxprüfung und `git diff --check` erfolgreich.
- Smoke-Test einschließlich neuer API auf einer Datenbankkopie; Buchungsanzahl und SQLite-Integrität werden geprüft.
- Browserprüfung bei 1440×1000 und 390×844: Bereichswechsel, Monatswechsel, freie Auswahl, Übernahme zu Transaktionen, Quartalsaggregation, Ausgabenperspektiven, Sparansicht, regelmäßige Buchungen und Termine, Ausschließen/Wiederherstellen einer Demo-Serie, Kategorie-Dialog und Escape-Rückkehr. Keine Browserfehler in der abschließenden Prüfung.
- Keine vollständige Screenreader- oder WCAG-Zertifizierung; die verwendeten nativen Controls, Labels, Fokuszustände und Datentabellen erleichtern eine spätere vertiefte Prüfung.

Die lokale Vorschau unter `http://127.0.0.1:8010` verwendet eine separate Datenbankkopie mit zusätzlichen, als Demo benannten Buchungen. Die Beispiele sind keine aktuellen persönlichen Ist-Werte. Die Screenshots zeigen diese Vorschau.

## Bildschirmbelege

Screenshots liegen ausschließlich lokal unter `docs/statistik-konzept/umsetzung/`; sie werden nicht ins Repository übertragen.





## Feinschliff nach Rückmeldung

- Kein Zurück-Button im Kategoriepanel. Ein echter Klick auf die Abdunklung schließt das Panel; Klicks im Panel und ein dort begonnener Drag schließen es nicht. Schließen-Symbol und Escape bleiben verfügbar.
- KPI-Icons verwenden denselben vorhandenen Icon-Satz wie Dashboard und frühere Statistikansichten. Die Formen sind in `statistics-icons.svg` zusammengefasst; es gibt keine neue externe Abhängigkeit.
- Doppelte Diagrammbeschreibungen, Einleitungssätze und wiederholte Bedienhinweise entfernt. Relevante Angaben zu Zeitraum, vorläufigen Vergleichen, Buchungsbeträgen und Datenstand bleiben erhalten.
- Balken zeigen die Beträge direkt, auch bei negativen Werten und gestapelten Segmenten. Linien mit bis zu acht Werten zeigen die Zahlen an den Punkten. Zielgeraden bleiben über ihre Legende beschriftet.
- Das Chart.js-Plugin berücksichtigt Datenlücken, Vorzeichen, Prozentwerte und mögliche Beschriftungsüberschneidungen. Dichte Balkenreihen können innerhalb des Diagramms horizontal gescrollt werden, damit Werte lesbar bleiben.
- Browsergeprüft: Quartalsansicht, positive/negative Änderungsbalken, gestapelte Balken, kurzer Sparverlauf und Panel-Innen-/Außenklick. JavaScript-Syntax und Diff-Format geprüft.



## Deployment-Ablauf

- Pi: `http://192.168.178.43:8001`
- Compose: `/home/shalm/haushaltsbuch-stack.yml`, Projekt `haushaltsbuch`
- Release: `/home/shalm/haushaltsbuch-releases/statistics-ux-20261016`
- Neues Image: `haushaltsbuch:statistics-ux-20261016`
- Vorheriges Image: `haushaltsbuch:6bf4faf-20261016`
- Compose-Sicherung: `/home/shalm/haushaltsbuch-stack.before-statistics-ux-20261016.yml`
- Konsistente SQLite-Sicherung: `/app/data/backup-before-statistics-ux-20261016.db`

Vor dem Wechsel werden Regressionstests und die Lese-Endpunkte mit einer separaten SQLite-Kopie im neuen Image geprüft. Danach erfolgt der Compose-Wechsel mit unverändertem Datenvolume. HTTP, Commit-Label, Quelldateien und Datenbankintegrität werden anschließend kontrolliert.

Rollback: Compose-Sicherung zurückkopieren und `docker compose -p haushaltsbuch -f /home/shalm/haushaltsbuch-stack.yml up -d --no-deps haushaltsbuch` ausführen. Die Datenbank bleibt dabei erhalten.
