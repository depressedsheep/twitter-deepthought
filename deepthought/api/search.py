"""This module provides the search functionality for the API module."""
import calendar
import os
import collections

from deepthought import helpers


def search(query):
    """Searches a list of files to find the frequency of the keyword in tweets over time.

    This is done by first getting the list of files to be searched, with the method :meth:`deepthought.search.get_dates_in_range`.
    If any of the files are missing, the dates will be omitted.

    Then, the corresponding search.txt files of the dates are downloaded and opened one by one. Due to the large file
    sizes and memory limitations, the search.txt fils are read in chunks of 10MB. The builtin function ``count`` is used
    to find the frequency of the keyword in the chunk of tweets. This frequency will then be added to an overall counter
    and added to a dict in the format ``frequency_dict[date] = frequency``.

    However, as the file is read in chunks, there exist a possibility that a keyword is cut off between chunks. For example,
    the keyword "123" will not be accounted for if the file "123*123***" was read in chunks of ["123*1", "23***"].
    Our solution is to take the last few letters of the previous chunk and append it to the next chunk. The number of
    letters is determined by len(query) - 1, which is the worst case scenario for a query to be cut off. Returning to
    the previous example, the number of letters will be len("123") - 1 = 2. Thus, the last :math:`2` letters of
    :math:`C_i` will be appended to :math:`C_{i+1}`, where :math:`C_i` is the :math:`i` th chunk and :math:`1 < i < N`
    where :math:`N` is the total number of chunks. So, the last 2 characters of "123*1": "*1" will be appended to
    23***": "*123***" when counting the frequency of keywords. In pythonic terms:

    .. math:: new\ string = C_{i-1} [-len(query):\ ] + C_{i}

    Args:
        query (str): The keyword to find

    Returns:
        ordered_freq (collections.OrderedDict): An ordered dict of (time, frequency) values
    """

    '''
        TODO:
            - fix code to allow simultaneous queries (uniq dl file locs)
    '''

    frequency = {}
    # List of files to be searched for
    file_list = get_dates_in_range("06-06-2015_01", "06-06-2015_21")

    bucket = helpers.S3Bucket()
    for file in file_list:
        # key_name = file + "/search.csv.bz2"
        # key = bucket.find_key(key_name)
        # file_path = os.path.join(config.working_dir, "TMP_search.txt.bz2")
        # if os.path.isfile(file_path):
        # os.remove(file_path)
        # bucket.download(key, file_path)
        # file_path = helpers.decompress_file(file_path)
        # search_f = open(file_path)
        # tweets = tweets_f.readlines()
        # frequency[file] = tweets.count(query)
        # search_f.close()
        # os.remove(file_path)

        file_path = os.path.join("tmp", file, "search.txt")
        if not os.path.isfile(file_path):
            continue
        search_f = open(file_path)
        f_chunks = helpers.read_file_in_chunks(search_f)
        curr_freq = 0
        t = ""
        for chunk in f_chunks:
            chunk = t + chunk
            curr_freq += chunk.count(query)
            tl = len(query) - 1
            t = chunk[-tl:]

        frequency[file] = curr_freq
        search_f.close()

    ordered_freq = collections.OrderedDict(sorted(frequency.items()))
    return ordered_freq


def get_dates_in_range(start, end):
    """Gets the list of dates, in increments of 1 hour, that falls within specified range

    Args:
        start (str): The starting date
        end (str): The ending date

    Returns:
        date_list (list): The list of dates

    Note:
        All dates are specified in the format "DD-MM-YYYY_HH"
    """
    date_list = []
    curr_date = ""
    day, month, year, hour = start[:2], start[3:5], start[6:10], start[-2:]
    day, month, year, hour = int(day), int(month), int(year), int(hour)

    while curr_date != end:
        curr_date = "%02d-%02d-%d_%02d" % (day, month, year, hour)
        date_list.append(curr_date)
        num_days_in_month = calendar.monthrange(year, month)
        hour += 1
        if hour >= 24:
            hour = 0
            day += 1
        if day > num_days_in_month:
            day = 0
            month += 1
        if month > 12:
            month = 1
            year += 1

    return date_list