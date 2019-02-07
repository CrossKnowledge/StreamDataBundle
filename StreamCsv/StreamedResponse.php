<?php
namespace CrossKnowledge\StreamDataBundle\StreamCsv;

use Symfony\Component\HttpFoundation\StreamedResponse;

class StreamedResponseextends extends StreamedResponse
{
    /**
     * Nb lines trigger to flush output and send CSV lines in getLinkedContentAction WS
     */
    const STREAM_QUERY_LIMIT = 1000;

    /**
     * @var int batch limit
     */
    private $queryLimit = 0;

    public function __construct($contentQuery, $status = 200, $headers = array(), $queryLimit = self::STREAM_QUERY_LIMIT)
    {
        parent::__construct(
            function() use ($contentQuery) {
                $this->calbackStream($contentQuery);
            },
            $status,
            array_merge($headers, [CkStreamReader::HEADER_CONTENT_COUNT => $contentQuery->count()])
        );
        $this->queryLimit = $queryLimit;

    }

    private function calbackStream(\ModelCriteria $contentQuery)
    {
        //Do all queries in a transaction to avoid insert / update issue between the first and the last query
        //otherwise contentCount should be false
        \Propel::getConnection()->beginTransaction();
        $contentQuery->setLimit($this->queryLimit);
        $offSet = 0;
        do {
            $contents = $contentQuery
                ->setOffset($offSet)
                ->find();
            foreach ($contents as $content) {
                if (fputcsv(fopen('php://output', 'w'), $content) == false) {
                    error_log('fputcsv failed on \'php://output\' with content : ' . serialize($content));
                }
            }
            flush();
            $offSet += $this->queryLimit;
        } while (count($contents) == $this->queryLimit);
        //Nothing to commit but we need to close transaction
        \Propel::getConnection()->commit();
    }
}