# Statistik- und Importkorrekturen, 11.09.2026

## Verhalten

- Neue Datenbanken erhalten korrekte Kategoriearten. Bestehende Einstellungen bleiben erhalten; verdächtige Arten werden als Datenqualitätsproblem angezeigt.
- CSV erkennt UTF-8/BOM und CP1252. Ungültige Beträge und fehlende Kopfzeilen führen zu Importfehlern. Eindeutig passende Vormerkungen werden in gebuchte Umsätze überführt; Ist-Auswertungen enthalten ausschließlich gebuchte Umsätze.
- Hanseatic-PDFs müssen Anfangs-, End- und Seitenübertragssalden erfüllen. Unvollständige oder nicht unterstützte Auszüge werden vor dem Schreiben abgewiesen. Eine Kartenabrechnung wird anhand von Buchungsart und Beschreibung erkannt; normale Gutschriften bleiben Erstattungen.
- Manuelle Kategorien und Bestandszuordnungen sind vor pauschaler Regelanwendung geschützt. Neue gelernte Zuordnungen berücksichtigen Quelle, Konto, Händler, Beschreibung und Richtung. Die Herkunft ist im Transaktions-API sichtbar.
- Budget, Zeitverlauf und Übersicht verwenden Nettokonsum einschließlich Erstattungen. Freie Vergleichszeiträume werden taggenau berechnet, Monatsbudgets anteilig skaliert.
- Kategorie-Nullmonate werden bei vorhandenen Konto-Beobachtungen ergänzt. Fehlende Gesamtmonate bleiben unbekannt, die Trendregression verwendet tatsächliche Kalenderabstände. Vergleichsbaselines verwenden nur frühere Monate.
- Wiederkehrende Zahlungen werden nach Quelle/Konto/Händler/Kategorie getrennt. Regelmäßige Monatsintervalle (1, 2, 3, 6, 12) bestimmen Fälligkeiten und Jahreskosten. Über längere Zeiträume wird jede Fälligkeit ergänzt, einschließlich bekannter Einnahmen und Sparraten. Lange überfällige Serien werden aus den aktiven Summen entfernt und in der Liste gekennzeichnet.
- Sparüberweisungsquote und Überschussquote sind getrennt benannt. Die kumulierte Ansicht zeigt Sparüberweisungen, keinen vollständigen Vermögensstand.
- Veraltete Kontodaten pausieren die laufende Hochrechnung. Die letzte Buchung ist ausdrücklich kein Vollständigkeitsnachweis.
- Unter Import können vollständige Kontozeiträume bestätigt und entfernt werden. Erfolgreich abgeglichene PDF-Zeiträume werden automatisch gespeichert.
- Die rückblickende Prognoseprüfung nutzt ausschließlich über alle Bankkonten bestätigte volle Monate, mit Stichtagen am 7., 14. und 21. Die Erkennung wiederkehrender Zahlungen sieht dabei keine späteren Umsätze. Angezeigt werden mittlere absolute Fehler des Terminmodells und der reinen Tageshochrechnung.

## Prüfung

```sh
python -m unittest discover -s tests -v
HANSEATIC_TEST_PDF=/absoluter/pfad/zum/auszug.pdf python -m unittest discover -s tests -v
DB_PATH=/pfad/zu/einer/wegwerfkopie.db python -m scripts.smoke_test
```

Der optionale reale PDF-Test ist auf den bereitgestellten August-Auszug 2026 zugeschnitten. Das PDF ist nicht im Repository. Geprüft wurden 44 Einzelbeträge, 43 Belastungen mit Summe -1.019,72 EUR, eine Abrechnung +169,22 EUR, Anfangssaldo -169,22 EUR, Endsaldo -1.019,72 EUR und fünf Saldo-Prüfpunkte. 34 Händler bleiben ohne sichere automatische Kategorie.

24 Tests bestehen lokal einschließlich des realen PDF-Tests. Auf dem Pi bestehen die 23 dateiunabhängigen Tests; die Migration und 13 Lese-Endpunkte wurden mit einer Kopie der 403 bestehenden Buchungen geprüft. Tests schreiben keine Buchungen in die produktive Datenbank.

## Grenzen

Es gibt noch keine empirisch kalibrierte Prognosebandbreite oder saisonales Modell. Variable Ausgaben werden weiterhin zeitanteilig hochgerechnet; die neue Terminlogik behandelt diskrete Zahlungen separat. Die Prognoseprüfung liefert die Grundlage, auf der komplexere Modelle verglichen werden können. Ohne bestätigte Abdeckung gibt es keine belastbare Fehlermessung. Historische manuelle Kategoriekorrekturen sind in rückblickenden Tests enthalten.

Automatischer Vormerkungsabgleich setzt unveränderten Betrag, Beschreibung und Transaktionsdatum voraus. Abweichende oder mehrdeutige Fälle müssen geprüft werden. Kontoidentität ist weiterhin Quelle plus Kontoname; gleich benannte Konten müssen getrennt benannt werden. Händler ohne eindeutige Information werden nicht anhand unbelegter Vermutungen kategorisiert. Bestehende Kontostände, Anlagebewertungen und Fremdwährungen sind kein Bestandteil der Vermögensberechnung.

## Pi-Deployment

- Adresse: http://192.168.178.43:8001
- Compose: `/home/shalm/haushaltsbuch-stack.yml`, Projekt `haushaltsbuch`
- Release-Quellen: `/home/shalm/haushaltsbuch-releases/codex-statistics-20260911`
- Image: `haushaltsbuch:statistics-20260911` (direkt auf dem Pi gebaut, nicht in die Registry gepusht)
- Vorheriges Image: `haushaltsbuch:before-statistics-20260911`
- Compose-Sicherung: `/home/shalm/haushaltsbuch-stack.before-statistics-20260911.yml`
- Konsistente SQLite-Sicherung im Datenvolume: `/app/data/backup-before-statistics-20260911-212329.db`

Für einen Code-Rollback das gesicherte Image in der Compose-Datei setzen und `docker compose -p haushaltsbuch -f /home/shalm/haushaltsbuch-stack.yml up -d --no-deps haushaltsbuch` ausführen. Das Datenvolume bleibt erhalten. Die additive Migration ist mit dem alten Code kompatibel. Eine Wiederherstellung der Datenbanksicherung würde nach dem Sicherungszeitpunkt vorgenommene Änderungen verwerfen und sollte nur gezielt erfolgen.
