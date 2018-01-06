"""Contains Data cluster specific data structures."""
# Import Built-Ins
import logging
import time
from collections import defaultdict

# Import Third-Party

# Import Homebrew

# Init Logging Facilities
log = logging.getLogger(__name__)


class AggregatedBook:
    """Aggregator class to generate a valid Level 2 Orderbook from a snapshot and diff quotes."""

    # pylint: disable=too-few-public-methods
    def __init__(self):
        """Initialize the instance."""
        self.bids = {}
        self.asks = {}

    def update(self, quote):
        """Update an entry in Book Level."""
        return self, quote


class AggregatedBookLevel:
    """Aggregator class to store and manage quotes based on its price and a custom identifier."""

    def __init__(self, reverse=False):
        """Initialize the instance."""
        self.reverse = reverse
        self.quotes = {}
        self.last = None
        self.level_size = 0.0
        self.price = None

    def update(self, quote, identifier):
        """Update an entry in Book Level."""
        # pylint: disable=unused-variable
        pair, price, size, side, uid, ts = quote
        if not self.price:
            self.price = price
        if not float(size):
            try:
                q = self.quotes.pop((price, identifier))
                self.level_size -= float(q[2])
            except KeyError:
                pass
        else:
            prev_vol = 0.0
            if (price, identifier) in self.quotes:
                try:
                    prev_vol = float(self.quotes[(price, identifier)][0][2])
                except Exception:
                    print((price, identifier), self.quotes[(price, identifier)])
                    raise

            new_vol = size
            delta = float(prev_vol) - float(new_vol)
            self.level_size -= delta
            self.quotes[(price, identifier)] = quote, identifier
        self.last = time.time()

    @property
    def level2(self):
        """Return the aggregated level size and price."""
        return self.price, self.level_size

    @property
    def level3(self):
        """Return L3 Book level, sorted by size (descending)."""
        return sorted([self.quotes[key] for key in self.quotes],
                      key=lambda x: float(x[0][2]))

    @property
    def tallest_order(self):
        """Return the tallest order by order size."""
        try:
            return self.level3[0]
        except IndexError:
            return None, None


class BidAggregatedBookLevel(AggregatedBookLevel):
    """Bid-side AggregatedBookLevel class."""

    def __init__(self):
        """Initialize the instance."""
        super(BidAggregatedBookLevel, self).__init__(reverse=True)

# pylint: disable=unnecessary-lambda,unused-variable


class HyperLedger:
    """Compound ledger consisting of Quotes from various exchanges."""

    def __init__(self, base_cur, quote_cur):
        """Initialize the instance."""
        self.bids = defaultdict(AggregatedBookLevel)
        self.asks = defaultdict(AggregatedBookLevel)
        self.pair = base_cur, quote_cur

    def update(self, quote, origin):
        """Update an entry in Book Level."""
        pair, price, size, side, uid, ts = quote
        book = self.bids if side == 'bid' else self.asks
        if not float(size) and price in book:
            try:
                book[price].update(quote, (price, origin))
            except KeyError:
                pass
            if not book[price].quotes:
                book.pop(price)
        else:
            book[price].update(quote, origin)

    def load(self, bids, asks, origin):
        """Load a snapshot into the ledger."""
        for quote in (*bids, *asks):
            pair, price, size, side, uid, ts = quote
            book = self.bids if side == 'bid' else self.asks
            book[price].update(quote, origin)

    def pretty_print(self):
        """Output content to console."""
        bid_keys = sorted(list(self.bids.keys()), key=lambda x: float(x), reverse=True)
        ask_keys = sorted(list(self.asks.keys()), key=lambda x: float(x), reverse=False)
        # BID | ASK
        # Layout of row:ORIGIN of tallest order SIZE, PRICE | PRICE, total SIZE, ORIGIN of
        # tallest order
        sorted_bids = []
        for limit_key in bid_keys:
            book_limit_level = self.bids[limit_key]
            tallest_order, origin = book_limit_level.tallest_order
            if tallest_order and origin:
                sorted_bids.append(['Bid', origin, book_limit_level.level_size,
                                    book_limit_level.price])

        sorted_asks = []
        for limit_key in ask_keys:
            book_limit_level = self.asks[limit_key]
            tallest_order, origin = book_limit_level.tallest_order
            if tallest_order and origin:
                sorted_asks.append([book_limit_level.price,
                                    book_limit_level.level_size, origin, 'Ask'])

        header = "%s ORDER BOOK:\n" % ''.join(self.pair).upper()
        table_fields = "SIDE\tORIGIN\tSIZE\tPRICE | PRICE\tSIZE\tORIGIN\tSIDE\n"
        table_row = "{}\t{}\t{}\t{} | {}\t{}\t{}\t{}\n"
        output = header + table_fields
        for i in range(min([20, len(sorted_asks), len(sorted_bids)])):
            output += table_row.format(*sorted_bids[i], *sorted_asks[i])
        print(output)
