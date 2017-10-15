from common import Scraper


class BaseScraper(Scraper):
    owner = 'simonw'
    repo = 'disaster-data'
    committer = {
        'name': 'irma-scraper',
        'email': 'irma-scraper@example.com',
    }
    slack_botname = 'Irma Scraper'
    slack_channel = '#shelter_scraper_data'


class BaseDeltaScraper(BaseScraper):
    """
    The fetch_data() method should return a list of dicts. Each dict
    should have a key that can be used to identify the row in that dict.

    Then you define a display_record(record) method that returns a string.
    """
    record_key = None
    show_changes = False
    noun = 'record'
    source_url = None
    slack_channel = None

    @property
    def display_name(self):
        return self.filepath.replace('.json', '')

    @property
    def noun_plural(self):
        return self.noun + 's'

    def create_message(self, new_records):
        return self.update_message([], new_records, 'Created')

    def update_message(self, old_records, new_records, verb='Updated'):
        previous_ids = [
            record[self.record_key] for record in old_records
        ]
        current_ids = [
            record[self.record_key] for record in new_records
        ]
        added_ids = [id for id in current_ids if id not in previous_ids]
        removed_ids = [id for id in previous_ids if id not in current_ids]

        message_blocks = []
        if added_ids:
            messages = []
            messages.append('%d new %s:' % (
                len(added_ids), self.noun if len(added_ids) == 1 else self.noun_plural
            ))
            for id in added_ids:
                record = [r for r in new_records if r[self.record_key] == id][0]
                messages.append(self.display_record(record))
            message_blocks.append(messages)

        if removed_ids:
            messages = []
            messages.append('%d %s removed:' % (
                len(removed_ids), self.noun if len(removed_ids) == 1 else self.noun_plural
            ))
            for id in removed_ids:
                record = [r for r in old_records if r[self.record_key] == id][0]
                messages.append(self.display_record(record))
            message_blocks.append(messages)

        # Add useful rendering of CHANGED records as well
        changed_records = []
        for new_record in new_records:
            try:
                old_record = [
                    r for r in old_records
                    if r[self.record_key] == new_record[self.record_key]
                ][0]
            except IndexError:
                continue
            changed_records.append((old_record, new_record))

        if self.show_changes and changed_records:
            messages = []
            messages.append('%d %s changed:' % (
                len(removed_ids), self.noun if len(removed_ids) == 1 else self.noun_plural
            ))
            for old_record, new_record in changed_records:
                messages.append(self.display_changes(old_record, new_record))
            message_blocks.append(messages)

        blocks = []
        for message_block in message_blocks:
            block = '\n'.join(message_block)
            blocks.append(block.strip())

        if self.source_url:
            blocks.append('Detected on %s' % self.source_url)

        body = '\n\n'.join(blocks)

        summary = []
        if added_ids:
            summary.append('%d %s added' % (
                len(added_ids), self.noun if len(added_ids) == 1 else self.noun_plural
            ))
        if removed_ids:
            summary.append('%d %s removed' % (
                len(removed_ids), self.noun if len(removed_ids) == 1 else self.noun_plural
            ))
        if changed_records:
            summary.append('%d %s changed' % (
                len(changed_records), self.noun if len(changed_records) == 1 else self.noun_plural
            ))
        if summary:
            summary_text = self.display_name + ': ' + (', '.join(summary))
        else:
            summary_text = '%s %s' % (verb, self.display_name)
        return summary_text + '\n\n' + body
