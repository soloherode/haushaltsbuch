# Kategoriezuordnung beim Import, 03.10.2026

## Befund und Änderung

Im letzten Import auf dem Pi waren fünf Buchungen manuell korrigiert worden.
Ein weiterer gespeicherter manueller Kontext widersprach einer allgemeinen
Händlerregel. Bisher hatten DB-Regeln Vorrang vor gelernten Korrekturen;
Sammelkorrekturen und Korrekturen ohne Händlername wurden nicht gelernt.

Die neue Zuordnung verwendet eindeutige manuelle Korrekturen zuerst, getrennt
nach Quelle, Konto, Händler, Buchungstext und Zahlungsrichtung. Groß-/Kleinschreibung,
Unicode-Darstellung und Leerzeichen werden vereinheitlicht; Referenznummern und
inhaltliche Unterschiede bleiben erhalten. Widersprüchliche Beispiele werden
nicht als eindeutige Kategorie übernommen. Einzel- und Sammelkorrekturen werden
beide gespeichert. Ältere Sammelkorrekturen werden aus explizit manuell
zugeordneten Buchungen berücksichtigt. Aufteilungen und veraltete Lernbeispiele
für aufgeteilte Einkäufe werden nicht als einzelne Kategorie verallgemeinert.

Bei gleicher Regelpriorität kommen Händler-/Zweckregeln vor allgemeinen
Zahlungsdienstleistern und längere Muster vor kürzeren. Explizite Prioritäten
bleiben erhalten. Kartenabrechnungen bleiben als Umbuchungen geschützt;
gelernte Zuordnungen werden durch „Regeln anwenden“ nicht überschrieben.

Standard-Schlüsselwörter brauchen Wortgrenzen, damit z. B. „Espresso“ nicht
auf „esso“ trifft und Referenzen nicht auf „o2“. Visa, PayPal und Riverty
begründen allein keine automatische Standard-Ausgabenkategorie. Vorhandene
benutzerdefinierte Regeln für diese Begriffe bleiben erhalten. Musikschule wird
standardmäßig Kinder zugeordnet; H&M, Zara und Zalando Kleidung. Individuelle
Zuordnungen zu Kinder haben bei passendem Kontext weiterhin Vorrang.

Die Importmeldung zählt ungeklärte Kategorien anhand der tatsächlich neu
importierten Endzuordnungen statt anhand der unveränderten Parserausgabe.

## Prüfung und Pi-Deployment

- Lokal und auf dem Pi: 54 Tests erfolgreich, ein optionaler PDF-Test übersprungen.
- Migration und 13 Lese-Endpunkte an einer Kopie geprüft, 450 Buchungen erhalten.
- Wiederverwendung aller fünf zuletzt korrigierten Kontexte und Vorrang vor
  widersprechenden Händlerregeln auf der Datenbankkopie bestätigt.
- Erneuter Import der fünf Buchungen auf der Kopie änderte keine Kategorien,
  Sperrmarkierungen oder Aufteilungen.
- Adresse: http://192.168.178.43:8001
- Image: `haushaltsbuch:categorization-20261003`
- Vorheriges Image/Basis: `haushaltsbuch:strict-recurring-20261003`
- Quellen: `/home/shalm/haushaltsbuch-releases/codex-categorization-20261003`
- Compose: `/home/shalm/haushaltsbuch-stack.yml`, Projekt `haushaltsbuch`
- Compose-Sicherung: `/home/shalm/haushaltsbuch-stack.before-categorization-20261003.yml`
- SQLite-Sicherung im Datenvolume: `/app/data/backup-before-categorization-20261003.db`

Vorhandene Buchungen werden beim Deployment nicht neu kategorisiert. Kleidung
für Erwachsene und Kinder lässt sich aus dem Händlernamen allein nicht
zuverlässig unterscheiden. Auch inhaltlich veränderte Zahlungszwecke und
Referenzen werden nicht automatisch gleichgesetzt.

Für einen Code-Rollback die Compose-Sicherung zurückkopieren und
`docker compose -p haushaltsbuch -f /home/shalm/haushaltsbuch-stack.yml up -d --no-deps haushaltsbuch`
ausführen; das Datenvolume bleibt bestehen.
