import React from 'react'
import styles from './index.module.css'

// Sorted by apacheId
const committers = [
  { githubId: 'ClSlaid', apacheId: 'cailue', name: 'Cai Lue', inPPMC: true },
  { githubId: 'Young-Flash', apacheId: 'dongyang', name: 'Dongyang Zheng', inPPMC: false },
  { githubId: 'G-XD', apacheId: 'gxd', name: 'Xiangdong', inPPMC: false },
  { githubId: 'Hexiaoqiao', apacheId: 'hexiaoqiao', name: 'Xiaoqiao He', inPPMC: true },
  { githubId: 'oowl', apacheId: 'junouyang', name: 'Jun Ouyang', inPPMC: false },
  { githubId: 'dqhl76', apacheId: 'liuqingyue', name: 'Liuqing Yue', inPPMC: false },
  { githubId: 'Zheaoli', apacheId: 'manjusaka', name: 'Zheao Li', inPPMC: false },
  { githubId: 'messense', apacheId: 'messense', name: 'Lusheng Lyu', inPPMC: false },
  { githubId: 'morristai', apacheId: 'morristai', name: 'Morris Tai', inPPMC: false },
  { githubId: 'WillemJiang', apacheId: 'ningjiang', name: 'Willem Ning Jiang', inPPMC: true },
  { githubId: 'PsiACE', apacheId: 'psiace', name: 'Chojan Shang', inPPMC: true },
  { githubId: 'silver-ymz', apacheId: 'silver', name: 'Mingzhuo Yin', inPPMC: true },
  { githubId: 'sundy-li', apacheId: 'sundyli', name: 'Sundy Li', inPPMC: true },
  { githubId: 'suyanhanx', apacheId: 'suyanhanx', name: 'Han Xu', inPPMC: true },
  { githubId: 'tedliu1', apacheId: 'tedliu', name: 'Ted Liu', inPPMC: true },
  { githubId: 'tisonkun', apacheId: 'tison', name: 'Zili Chen', inPPMC: true },
  { githubId: 'wu-sheng', apacheId: 'wusheng', name: 'Sheng Wu', inPPMC: true },
  { githubId: 'Xuanwo', apacheId: 'xuanwo', name: 'Hao Ding', inPPMC: true },
  { githubId: 'Ji-Xinyou', apacheId: 'xyji', name: 'Xinyou Ji', inPPMC: false }
]

export default function Committers() {
  return <>
    <table>
      <thead>
      <tr>
        <th><b>Avatar</b></th>
        <th><b>Name</b></th>
        <th><b>Apache ID</b></th>
        <th><b>GitHub ID</b></th>
      </tr>
      </thead>
      <tbody>
      {committers
        .sort((c0, c1) => c0.apacheId.localeCompare(c1.apacheId))
        .map(v => (
          <tr key={v.name}>
            <td><img width={64} className={styles.contributorAvatar}
                     src={`https://github.com/${v.githubId}.png`} alt={v.name}/></td>
            <td>{v.inPPMC ? <b>{v.name}</b> : v.name}</td>
            <td><a target='_blank' href={`https://people.apache.org/phonebook.html?uid=${v.apacheId}`}>{v.apacheId}</a></td>
            <td><a target='_blank' href={`https://github.com/${v.githubId}`}>{v.githubId}</a></td>
          </tr>
        ))}
      </tbody>
    </table>
  </>
}
