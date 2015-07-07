"""This module provides the search functionality for the API module."""
import calendar
import collections
import uuid
import os
import shutil
import json

from deepthought import config, helpers


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

    frequency = {}

    gen_uniq_dir = lambda: os.path.join(config.working_dir, str(uuid.uuid4()))
    tmp_dir = gen_uniq_dir()
    while os.path.isdir(tmp_dir):
        tmp_dir = gen_uniq_dir()

    os.mkdir(tmp_dir)

    b = helpers.S3Bucket()
    kl = b.find_keys("search.json")
    helpers.S3Bucket.download_async(kl, tmp_dir)

    for subdir, dirs, files in os.walk(tmp_dir):
        for file in files:
            file_path = os.path.join(subdir, file)
            if file_path.lower().endswith(".bz2"):
                file_path = helpers.decompress_file(file_path)
            with open(file_path, 'r') as json_file:
                freq_dict = json.load(json_file)
                date = subdir.split(os.sep)[-1]
                frequency[date] = freq_dict[query]

    # shutil.rmtree(tmp_dir)
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