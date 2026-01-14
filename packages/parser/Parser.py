import feedparser


class Parser:
    def parse(self, xml_text: str) -> list[dict]:
        feed = feedparser.parse(xml_text)

        items = []
        for entry in feed.entries:
            items.append(
                {
                    "guid": getattr(entry, "id", getattr(entry, "link", "")),
                    "title": getattr(entry, "title", ""),
                    "link": getattr(entry, "link", ""),
                    "published": getattr(entry, "published", None),
                    "summary": getattr(entry, "summary", ""),
                }
            )
        return items
