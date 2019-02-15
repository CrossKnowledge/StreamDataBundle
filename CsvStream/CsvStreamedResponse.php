<?php
namespace CrossKnowledge\StreamDataBundle\CsvStream;

use Symfony\Component\HttpFoundation\StreamedResponse;

/**
 * Class CsvStreamedResponse
 * @package CrossKnowledge\StreamDataBundle\CsvStream
 */
class CsvStreamedResponse extends StreamedResponse
{
    /**
     * Nb lines trigger to flush output and send CSV lines in getLinkedContentAction WS
     */
    const STREAM_QUERY_LIMIT = 1000;

    /**
     * @var int batch limit
     */
    private $queryLimit = 0;

    /**
     * CsvStreamedResponse constructor.
     * @param \ModelCriteria $contentQuery
     * @param int $status
     * @param array $headers
     * @param int $queryLimit
     */
    public function __construct(\ModelCriteria $contentQuery, $status = 200, $headers = array(), $queryLimit = self::STREAM_QUERY_LIMIT)
    {
        parent::__construct(
            function() use ($contentQuery) {
                $this->callbackStream($contentQuery);
            },
            $status,
            array_merge($headers, [CsvStreamReader::HEADER_CONTENT_COUNT => $contentQuery->count()])
        );
        $this->queryLimit = $queryLimit;

    }

    /**
     * @param \ModelCriteria $contentQuery
     * @throws \PropelException
     */
    private function callbackStream(\ModelCriteria $contentQuery)
    {
        //Do all queries in a transaction to avoid insert / update issue between the first and the last query
        //otherwise contentCount should be false
        \Propel::getConnection()->beginTransaction();
        $contentQuery->setLimit($this->queryLimit);
        $offSet = 0;
        $bufferHandler = fopen('php://output', 'w');
        do {
            $contents = $contentQuery
                ->setOffset($offSet)
                ->find();
            foreach ($contents as $content) {
                if (fputcsv($bufferHandler, $content) == false) {
                    error_log('fputcsv failed on \'php://output\' with content : ' . serialize($content));
                }
            }

            flush();
            $offSet += $this->queryLimit;
        } while (count($contents) == $this->queryLimit);
        fclose($bufferHandler);
        //Nothing to commit but we need to close transaction
        \Propel::getConnection()->commit();
    }
}